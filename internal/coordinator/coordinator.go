// Package coordinator runs one canonical job on two workers and accepts only
// an independently retrieved state whose hash matches both commitments.
package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/0xciph3r/mini-verde/internal/job"
	"github.com/0xciph3r/mini-verde/internal/protocol"
)

// Status describes whether a result is ready for use or requires the dispute
// protocol implemented in a later milestone.
type Status string

const (
	Accepted     Status = "Accepted"
	NeedsDispute Status = "NeedsDispute"
)

// Endpoint identifies one worker and its loopback HTTP address.
type Endpoint struct {
	ID  string
	URL string
}

// Commitment is one worker's binding of a trace root to a final-state hash.
type Commitment struct {
	WorkerID       string
	AttemptID      uint64
	Root           protocol.Digest
	FinalStateHash protocol.Digest
}

// Finding records a protocol verdict produced while validating a worker.
type Finding struct {
	WorkerID string
	Verdict  string
}

// Result contains either two disagreeing commitments or one accepted state.
type Result struct {
	Status        Status
	JobID         protocol.Digest
	State         job.State
	StateHash     protocol.Digest
	Commitments   []Commitment
	Findings      []Finding
	CleanupErrors []string
}

// Coordinator talks to exactly two configured workers.
type Coordinator struct {
	endpoints [2]Endpoint
	client    *http.Client
	limits    protocol.Limits
	attempts  atomic.Uint64
}

// New validates a two-worker loopback configuration.
func New(endpoints []Endpoint, client *http.Client, limits protocol.Limits) (*Coordinator, error) {
	if len(endpoints) != 2 {
		return nil, fmt.Errorf("coordinator requires exactly two workers, got %d", len(endpoints))
	}
	if client == nil {
		return nil, fmt.Errorf("HTTP client is required")
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	var validated [2]Endpoint
	for i, endpoint := range endpoints {
		parsed, err := validateEndpoint(endpoint)
		if err != nil {
			return nil, fmt.Errorf("worker %d: %w", i, err)
		}
		validated[i] = parsed
	}
	if validated[0].ID == validated[1].ID {
		return nil, fmt.Errorf("worker IDs must be distinct")
	}
	ownedClient := *client
	ownedClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Coordinator{endpoints: validated, client: &ownedClient, limits: limits}, nil
}

func validateEndpoint(endpoint Endpoint) (Endpoint, error) {
	if strings.TrimSpace(endpoint.ID) == "" || len(endpoint.ID) > 128 {
		return Endpoint{}, fmt.Errorf("worker ID must contain 1 to 128 characters")
	}
	parsed, err := url.Parse(endpoint.URL)
	if err != nil {
		return Endpoint{}, fmt.Errorf("invalid URL: %w", err)
	}
	if parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return Endpoint{}, fmt.Errorf("worker URL must be a plain HTTP origin")
	}
	host := parsed.Hostname()
	address := net.ParseIP(host)
	if !strings.EqualFold(host, "localhost") && (address == nil || !address.IsLoopback()) {
		return Endpoint{}, fmt.Errorf("worker URL must use a loopback host")
	}
	endpoint.URL = strings.TrimSuffix(endpoint.URL, "/")
	return endpoint, nil
}

// Run dispatches a job concurrently, compares final-state hashes, retrieves
// and validates an agreed state, and then releases both worker attempts.
func (coordinator *Coordinator) Run(ctx context.Context, spec job.JobSpec) (Result, error) {
	if err := coordinator.limits.ValidateJob(spec); err != nil {
		return Result{}, err
	}
	machine, err := job.NewMachine(spec)
	if err != nil {
		return Result{}, err
	}
	encodedJob, err := job.MarshalSpec(spec)
	if err != nil {
		return Result{}, err
	}
	jobID := machine.ID()
	result := Result{JobID: jobID, Commitments: make([]Commitment, 2)}

	type execution struct {
		commitment Commitment
		err        error
	}
	executions := make([]execution, 2)
	var group sync.WaitGroup
	group.Add(2)
	for i := range coordinator.endpoints {
		i := i
		attemptID := coordinator.attempts.Add(1)
		go func() {
			defer group.Done()
			executions[i].commitment, executions[i].err = coordinator.execute(
				ctx, coordinator.endpoints[i], jobID, attemptID, encodedJob,
			)
		}()
	}
	group.Wait()
	for i, execution := range executions {
		if execution.err != nil {
			return Result{}, fmt.Errorf("execute on %s: %w", coordinator.endpoints[i].ID, execution.err)
		}
		result.Commitments[i] = execution.commitment
	}

	if result.Commitments[0].FinalStateHash != result.Commitments[1].FinalStateHash {
		result.Status = NeedsDispute
		return result, nil
	}

	agreedHash := result.Commitments[0].FinalStateHash
	var retrievalErrors []error
	for i, commitment := range result.Commitments {
		state, badState, err := coordinator.fetchAndValidateState(
			ctx, coordinator.endpoints[i], jobID, commitment.AttemptID, machine, agreedHash,
		)
		if err == nil {
			result.Status = Accepted
			result.State = state
			result.StateHash = agreedHash
			result.CleanupErrors = coordinator.closeAttempts(ctx, jobID, result.Commitments)
			return result, nil
		}
		if badState {
			result.Findings = append(result.Findings, Finding{
				WorkerID: commitment.WorkerID,
				Verdict:  protocol.VerdictBadState,
			})
		}
		retrievalErrors = append(retrievalErrors, fmt.Errorf("%s: %w", commitment.WorkerID, err))
	}
	return Result{}, fmt.Errorf("no worker returned the agreed final state: %w", errors.Join(retrievalErrors...))
}

func (coordinator *Coordinator) execute(
	ctx context.Context,
	endpoint Endpoint,
	jobID protocol.Digest,
	attemptID uint64,
	encodedJob []byte,
) (Commitment, error) {
	meta := protocol.Meta{JobID: protocol.EncodeDigest(jobID), AttemptID: attemptID, Round: 0}
	request := protocol.ExecuteRequest{Meta: meta, Job: encodedJob}
	var response protocol.ExecuteResponse
	if err := coordinator.post(ctx, endpoint.URL+protocol.ExecutePath, request, &response); err != nil {
		return Commitment{}, err
	}
	if !protocol.SameMeta(response.Meta, meta) {
		return Commitment{}, fmt.Errorf("response metadata does not match request")
	}
	if response.WorkerID != endpoint.ID {
		return Commitment{}, fmt.Errorf("response worker ID %q, want %q", response.WorkerID, endpoint.ID)
	}
	root, err := protocol.DecodeDigest(response.Root)
	if err != nil {
		return Commitment{}, fmt.Errorf("invalid root: %w", err)
	}
	finalHash, err := protocol.DecodeDigest(response.FinalStateHash)
	if err != nil {
		return Commitment{}, fmt.Errorf("invalid final-state hash: %w", err)
	}
	return Commitment{
		WorkerID:       response.WorkerID,
		AttemptID:      attemptID,
		Root:           root,
		FinalStateHash: finalHash,
	}, nil
}

func (coordinator *Coordinator) fetchAndValidateState(
	ctx context.Context,
	endpoint Endpoint,
	jobID protocol.Digest,
	attemptID uint64,
	machine *job.Machine,
	wantHash protocol.Digest,
) (job.State, bool, error) {
	meta := protocol.Meta{JobID: protocol.EncodeDigest(jobID), AttemptID: attemptID, Round: 1}
	var response protocol.FinalStateResponse
	if err := coordinator.post(ctx, endpoint.URL+protocol.FinalStatePath, protocol.FinalStateRequest{Meta: meta}, &response); err != nil {
		return job.State{}, false, err
	}
	if !protocol.SameMeta(response.Meta, meta) {
		return job.State{}, false, fmt.Errorf("response metadata does not match request")
	}
	if response.WorkerID != endpoint.ID {
		return job.State{}, false, fmt.Errorf("response worker ID %q, want %q", response.WorkerID, endpoint.ID)
	}
	state, err := machine.UnmarshalState(response.State)
	if err != nil {
		return job.State{}, true, fmt.Errorf("invalid canonical state: %w", err)
	}
	if state.Step != machine.LeafCount()-1 {
		return job.State{}, true, fmt.Errorf("state step %d, want %d", state.Step, machine.LeafCount()-1)
	}
	stateHash, err := machine.HashState(state)
	if err != nil {
		return job.State{}, true, fmt.Errorf("hash state: %w", err)
	}
	if stateHash != wantHash {
		return job.State{}, true, fmt.Errorf("state hash does not match agreed commitment")
	}
	return state, false, nil
}

func (coordinator *Coordinator) closeAttempts(
	ctx context.Context,
	jobID protocol.Digest,
	commitments []Commitment,
) []string {
	errorsByWorker := make([]error, len(commitments))
	var group sync.WaitGroup
	group.Add(len(commitments))
	for i, commitment := range commitments {
		i, commitment := i, commitment
		go func() {
			defer group.Done()
			meta := protocol.Meta{JobID: protocol.EncodeDigest(jobID), AttemptID: commitment.AttemptID, Round: 2}
			var response protocol.CloseResponse
			if err := coordinator.post(ctx, coordinator.endpoints[i].URL+protocol.ClosePath, protocol.CloseRequest{Meta: meta}, &response); err != nil {
				errorsByWorker[i] = err
				return
			}
			if !protocol.SameMeta(response.Meta, meta) || response.WorkerID != commitment.WorkerID || !response.Closed {
				errorsByWorker[i] = fmt.Errorf("invalid close acknowledgement")
			}
		}()
	}
	group.Wait()
	var messages []string
	for i, err := range errorsByWorker {
		if err != nil {
			messages = append(messages, fmt.Sprintf("%s: %v", commitments[i].WorkerID, err))
		}
	}
	return messages
}

func (coordinator *Coordinator) post(ctx context.Context, address string, requestValue, responseValue any) error {
	payload, err := json.Marshal(requestValue)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	if int64(len(payload)) > coordinator.limits.MaxBodyBytes {
		return fmt.Errorf("request exceeds %d-byte protocol limit", coordinator.limits.MaxBodyBytes)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, address, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	request.Header.Set("Content-Type", protocol.JSONContentType)
	request.Header.Set("Accept", protocol.JSONContentType)
	response, err := coordinator.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != protocol.JSONContentType {
		return fmt.Errorf("response Content-Type must be application/json")
	}
	limited := io.LimitReader(response.Body, coordinator.limits.MaxBodyBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if int64(len(data)) > coordinator.limits.MaxBodyBytes {
		return fmt.Errorf("response exceeds %d-byte protocol limit", coordinator.limits.MaxBodyBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var failure protocol.ErrorResponse
		if err := decodeOne(data, &failure); err != nil {
			return fmt.Errorf("worker returned HTTP %d with invalid error body", response.StatusCode)
		}
		return fmt.Errorf("worker returned HTTP %d %s: %s", response.StatusCode, failure.Code, failure.Message)
	}
	if err := decodeOne(data, responseValue); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func decodeOne(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("expected one JSON value")
	}
	return nil
}
