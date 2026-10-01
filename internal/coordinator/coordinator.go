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
	"time"

	"github.com/0xciph3r/mini-verde/internal/job"
	"github.com/0xciph3r/mini-verde/internal/protocol"
)

// Status describes whether a result is ready for use or requires the dispute
// protocol implemented in a later milestone.
type Status string

const (
	Accepted Status = "Accepted"
	Rejected Status = "Rejected"

	BasisFinalStateAgreement = "FinalStateAgreement"
	BasisLoserProvenFaulty   = "LoserProvenFaulty"
)

var errInvalidWorkerResponse = errors.New("invalid worker response")

const (
	DefaultRequestTimeout = 30 * time.Second
	DefaultJobTimeout     = 2 * time.Minute
)

// Config contains coordinator-owned resource and deadline policy.
type Config struct {
	Limits         protocol.Limits
	RequestTimeout time.Duration
	JobTimeout     time.Duration
}

func DefaultConfig() Config {
	return Config{
		Limits:         protocol.DefaultLimits(),
		RequestTimeout: DefaultRequestTimeout,
		JobTimeout:     DefaultJobTimeout,
	}
}

func (config Config) validate() error {
	if err := config.Limits.Validate(); err != nil {
		return err
	}
	if config.RequestTimeout <= 0 || config.JobTimeout <= 0 {
		return fmt.Errorf("request and job timeouts must be positive")
	}
	return nil
}

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
	WorkerID string  `json:"worker_id"`
	Verdict  string  `json:"verdict"`
	Step     *uint64 `json:"step,omitempty"`
}

type AcceptanceBasis struct {
	Kind         string `json:"kind"`
	FaultyWorker string `json:"faulty_worker,omitempty"`
	Verdict      string `json:"verdict,omitempty"`
}

type DisputeCost struct {
	StepHashRounds               uint64            `json:"step_hash_rounds"`
	BisectionRounds              uint64            `json:"bisection_rounds"`
	WorkerRecomputeStepsExpected map[string]uint64 `json:"worker_recompute_steps_expected_from_schedule"`
	RefereeSteps                 uint64            `json:"referee_steps"`
	LogicalRounds                uint64            `json:"logical_rounds"`
}

type DisputeSummary struct {
	LowStep  uint64      `json:"low_step"`
	HighStep uint64      `json:"high_step"`
	WinnerID string      `json:"winner_id,omitempty"`
	Cost     DisputeCost `json:"cost"`
}

// Result contains either two disagreeing commitments or one accepted state.
type Result struct {
	Status         Status
	JobID          protocol.Digest
	State          job.State
	StateHash      protocol.Digest
	Commitments    []Commitment
	Findings       []Finding
	OverallVerdict string
	Acceptance     *AcceptanceBasis
	Dispute        *DisputeSummary
	CleanupErrors  []string
}

// Coordinator talks to exactly two configured workers.
type Coordinator struct {
	endpoints      [2]Endpoint
	client         *http.Client
	limits         protocol.Limits
	requestTimeout time.Duration
	jobTimeout     time.Duration
	attempts       atomic.Uint64
}

// New validates a two-worker loopback configuration.
func New(endpoints []Endpoint, client *http.Client, limits protocol.Limits) (*Coordinator, error) {
	config := DefaultConfig()
	config.Limits = limits
	return NewWithConfig(endpoints, client, config)
}

// NewWithConfig validates a two-worker loopback configuration and deadline
// policy.
func NewWithConfig(endpoints []Endpoint, client *http.Client, config Config) (*Coordinator, error) {
	if len(endpoints) != 2 {
		return nil, fmt.Errorf("coordinator requires exactly two workers, got %d", len(endpoints))
	}
	if client == nil {
		return nil, fmt.Errorf("HTTP client is required")
	}
	if err := config.validate(); err != nil {
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
	return &Coordinator{
		endpoints:      validated,
		client:         &ownedClient,
		limits:         config.Limits,
		requestTimeout: config.RequestTimeout,
		jobTimeout:     config.JobTimeout,
	}, nil
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
	ctx, cancel := context.WithTimeout(ctx, coordinator.jobTimeout)
	defer cancel()
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
		result.Commitments[i] = execution.commitment
		if execution.err != nil {
			return result, fmt.Errorf("execute on %s: %w", coordinator.endpoints[i].ID, execution.err)
		}
	}

	if result.Commitments[0].FinalStateHash != result.Commitments[1].FinalStateHash {
		return coordinator.resolveDispute(ctx, machine, result)
	}

	agreedHash := result.Commitments[0].FinalStateHash
	var retrievalErrors []error
	for i, commitment := range result.Commitments {
		state, badState, err := coordinator.fetchAndValidateState(
			ctx, coordinator.endpoints[i], jobID, commitment.AttemptID, machine, agreedHash,
		)
		if err == nil {
			closeRound := uint64(2)
			if len(result.Findings) != 0 {
				opening := coordinator.requestOpening(
					ctx, coordinator.endpoints[i], jobID, commitment,
					machine.LeafCount(), machine.LeafCount()-1, 2,
				)
				if opening.err != nil {
					return result, fmt.Errorf("open fallback final state on %s: %w", commitment.WorkerID, opening.err)
				}
				if opening.bad || opening.hash != agreedHash {
					result.Findings = append(result.Findings, newFinding(
						commitment.WorkerID, protocol.VerdictBadOpening, machine.LeafCount()-1,
					))
					result.Status = Rejected
					result.CleanupErrors = coordinator.closeAttempts(ctx, jobID, result.Commitments, 3)
					return result, nil
				}
				closeRound = 3
			}
			result.Status = Accepted
			result.State = state
			result.StateHash = agreedHash
			if len(result.Findings) == 0 {
				result.Acceptance = &AcceptanceBasis{Kind: BasisFinalStateAgreement}
			} else {
				finding := result.Findings[0]
				result.Acceptance = &AcceptanceBasis{
					Kind: BasisLoserProvenFaulty, FaultyWorker: finding.WorkerID, Verdict: finding.Verdict,
				}
			}
			result.CleanupErrors = coordinator.closeAttempts(ctx, jobID, result.Commitments, closeRound)
			return result, nil
		}
		if badState {
			result.Findings = append(result.Findings, newFinding(
				commitment.WorkerID, protocol.VerdictBadState, machine.LeafCount()-1,
			))
		}
		retrievalErrors = append(retrievalErrors, fmt.Errorf("%s: %w", commitment.WorkerID, err))
	}
	if len(result.Findings) == len(result.Commitments) {
		result.Status = Rejected
		result.CleanupErrors = coordinator.closeAttempts(ctx, jobID, result.Commitments, 2)
		return result, nil
	}
	return result, fmt.Errorf("no worker returned the agreed final state: %w", errors.Join(retrievalErrors...))
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
	return coordinator.fetchAndValidateFinalState(ctx, endpoint, jobID, attemptID, machine, wantHash, 1)
}

func (coordinator *Coordinator) fetchAndValidateFinalState(
	ctx context.Context,
	endpoint Endpoint,
	jobID protocol.Digest,
	attemptID uint64,
	machine *job.Machine,
	wantHash protocol.Digest,
	round uint64,
) (job.State, bool, error) {
	meta := protocol.Meta{JobID: protocol.EncodeDigest(jobID), AttemptID: attemptID, Round: round}
	var response protocol.FinalStateResponse
	if err := coordinator.post(ctx, endpoint.URL+protocol.FinalStatePath, protocol.FinalStateRequest{Meta: meta}, &response); err != nil {
		if errors.Is(err, errInvalidWorkerResponse) {
			return job.State{}, true, err
		}
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
	round uint64,
) []string {
	errorsByWorker := make([]error, len(commitments))
	var group sync.WaitGroup
	group.Add(len(commitments))
	for i, commitment := range commitments {
		i, commitment := i, commitment
		go func() {
			defer group.Done()
			meta := protocol.Meta{JobID: protocol.EncodeDigest(jobID), AttemptID: commitment.AttemptID, Round: round}
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
	ctx, cancel := context.WithTimeout(ctx, coordinator.requestTimeout)
	defer cancel()
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

	limited := io.LimitReader(response.Body, coordinator.limits.MaxBodyBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if int64(len(data)) > coordinator.limits.MaxBodyBytes {
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return fmt.Errorf("worker returned oversized HTTP %d response", response.StatusCode)
		}
		return fmt.Errorf("%w: body exceeds %d-byte protocol limit", errInvalidWorkerResponse, coordinator.limits.MaxBodyBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var failure protocol.ErrorResponse
		if err := decodeOne(data, &failure); err != nil {
			return fmt.Errorf("worker returned HTTP %d", response.StatusCode)
		}
		return fmt.Errorf("worker returned HTTP %d %s: %s", response.StatusCode, failure.Code, failure.Message)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != protocol.JSONContentType {
		return fmt.Errorf("%w: Content-Type must be application/json", errInvalidWorkerResponse)
	}
	if err := decodeOne(data, responseValue); err != nil {
		return fmt.Errorf("%w: %v", errInvalidWorkerResponse, err)
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
