// Command coordinator submits one canonical Mini-Verde job to two workers.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/0xciph3r/mini-verde/internal/coordinator"
	"github.com/0xciph3r/mini-verde/internal/job"
	"github.com/0xciph3r/mini-verde/internal/protocol"
)

const maxJobFileBytes = 8 << 20

type workerFlags []string

func (values *workerFlags) String() string { return strings.Join(*values, ",") }

func (values *workerFlags) Set(value string) error {
	*values = append(*values, value)
	return nil
}

type output struct {
	Status         coordinator.Status           `json:"status"`
	JobID          string                       `json:"job_id"`
	FinalStateHash string                       `json:"final_state_hash,omitempty"`
	Roots          []string                     `json:"roots"`
	Findings       []coordinator.Finding        `json:"findings,omitempty"`
	OverallVerdict string                       `json:"overall_verdict,omitempty"`
	Acceptance     *coordinator.AcceptanceBasis `json:"acceptance,omitempty"`
	Dispute        *coordinator.DisputeSummary  `json:"dispute,omitempty"`
	CleanupErrors  []string                     `json:"cleanup_errors,omitempty"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "coordinator: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	flags := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	jobPath := flags.String("job", "", "path to a JSON job specification")
	requestTimeout := flags.Duration("request-timeout", coordinator.DefaultRequestTimeout, "deadline for each worker HTTP call")
	jobTimeout := flags.Duration("job-timeout", coordinator.DefaultJobTimeout, "overall job deadline")
	var workers workerFlags
	flags.Var(&workers, "worker", "worker endpoint as id=http://host:port (repeat twice)")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	if *jobPath == "" {
		return fmt.Errorf("--job is required")
	}
	endpoints, err := parseWorkers(workers)
	if err != nil {
		return err
	}
	spec, err := readJob(*jobPath)
	if err != nil {
		return err
	}
	config := coordinator.DefaultConfig()
	config.Limits = protocol.DefaultLimits()
	config.RequestTimeout = *requestTimeout
	config.JobTimeout = *jobTimeout
	client, err := coordinator.NewWithConfig(endpoints, &http.Client{}, config)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	result, err := client.Run(ctx, spec)
	if err != nil {
		return err
	}

	report := output{
		Status:         result.Status,
		JobID:          protocol.EncodeDigest(result.JobID),
		Roots:          make([]string, len(result.Commitments)),
		Findings:       result.Findings,
		OverallVerdict: result.OverallVerdict,
		Acceptance:     result.Acceptance,
		Dispute:        result.Dispute,
		CleanupErrors:  result.CleanupErrors,
	}
	if result.Status == coordinator.Accepted {
		report.FinalStateHash = protocol.EncodeDigest(result.StateHash)
	}
	for i, commitment := range result.Commitments {
		report.Roots[i] = protocol.EncodeDigest(commitment.Root)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func parseWorkers(values []string) ([]coordinator.Endpoint, error) {
	if len(values) != 2 {
		return nil, fmt.Errorf("exactly two --worker values are required")
	}
	endpoints := make([]coordinator.Endpoint, len(values))
	for i, value := range values {
		id, address, found := strings.Cut(value, "=")
		if !found || id == "" || address == "" {
			return nil, fmt.Errorf("worker %q must have the form id=http://host:port", value)
		}
		endpoints[i] = coordinator.Endpoint{ID: id, URL: address}
	}
	return endpoints, nil
}

func readJob(path string) (job.JobSpec, error) {
	file, err := os.Open(path)
	if err != nil {
		return job.JobSpec{}, fmt.Errorf("open job: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxJobFileBytes+1))
	if err != nil {
		return job.JobSpec{}, fmt.Errorf("read job: %w", err)
	}
	if len(data) > maxJobFileBytes {
		return job.JobSpec{}, fmt.Errorf("job file exceeds %d bytes", maxJobFileBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var spec job.JobSpec
	if err := decoder.Decode(&spec); err != nil {
		return job.JobSpec{}, fmt.Errorf("decode job: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return job.JobSpec{}, fmt.Errorf("job file must contain one JSON value")
	}
	if _, err := job.NewMachine(spec); err != nil {
		return job.JobSpec{}, err
	}
	return spec, nil
}
