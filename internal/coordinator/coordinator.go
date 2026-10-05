// Package coordinator runs one canonical job on an ordered worker pool and
// accepts only an independently retrieved state whose hash matches a verified
// commitment.
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
	"github.com/0xciph3r/mini-verde/internal/wal"
)

// Status describes whether a result is accepted, rejected by objective
// evidence, or unresolved after exhausting its liveness budget.
type Status string

const (
	Accepted   Status = "Accepted"
	Rejected   Status = "Rejected"
	Unresolved Status = "Unresolved"

	BasisFinalStateAgreement = "FinalStateAgreement"
	BasisLoserProvenFaulty   = "LoserProvenFaulty"
)

var errInvalidWorkerResponse = errors.New("invalid worker response")

var (
	errWorkerUnavailable = errors.New("worker unavailable")
	errRequestDeadline   = errors.New("worker request deadline")
	errJobDeadline       = errors.New("job deadline")
)

const (
	DefaultRequestTimeout = 30 * time.Second
	DefaultJobTimeout     = 2 * time.Minute
	DefaultRequestRetries = 2
)

// Config contains coordinator-owned resource and deadline policy.
type Config struct {
	Limits         protocol.Limits
	RequestTimeout time.Duration
	JobTimeout     time.Duration
	RequestRetries int
	WALPath        string
	Verifier       Verifier
}

func DefaultConfig() Config {
	return Config{
		Limits:         protocol.DefaultLimits(),
		RequestTimeout: DefaultRequestTimeout,
		JobTimeout:     DefaultJobTimeout,
		RequestRetries: DefaultRequestRetries,
		Verifier:       StrictVerifier{},
	}
}

func (config Config) validate() error {
	if err := config.Limits.Validate(); err != nil {
		return err
	}
	if config.RequestTimeout <= 0 || config.JobTimeout <= 0 || config.RequestRetries < 0 {
		return fmt.Errorf("request and job timeouts must be positive and retries must not be negative")
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
	Phase    string  `json:"phase,omitempty"`
}

type AttemptOutcome string

const (
	AttemptReady      AttemptOutcome = "Ready"
	AttemptAccepted   AttemptOutcome = "Accepted"
	AttemptFaulty     AttemptOutcome = "Faulty"
	AttemptTimedOut   AttemptOutcome = "Timeout"
	AttemptUnresolved AttemptOutcome = "Unresolved"
)

// AttemptSummary is the coordinator's ordered audit record for one dispatch.
type AttemptSummary struct {
	WorkerID       string         `json:"worker_id"`
	AttemptID      uint64         `json:"attempt_id"`
	PairGeneration uint64         `json:"pair_generation,omitempty"`
	Outcome        AttemptOutcome `json:"outcome"`
	Root           string         `json:"root,omitempty"`
	FinalStateHash string         `json:"final_state_hash,omitempty"`
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
	LowStep        uint64      `json:"low_step"`
	HighStep       uint64      `json:"high_step"`
	WinnerID       string      `json:"winner_id,omitempty"`
	PairGeneration uint64      `json:"pair_generation"`
	Cost           DisputeCost `json:"cost"`
}

// Result contains either two disagreeing commitments or one accepted state.
type Result struct {
	Status          Status
	JobID           protocol.Digest
	State           job.State
	StateHash       protocol.Digest
	Commitments     []Commitment
	Findings        []Finding
	OverallVerdict  string
	Acceptance      *AcceptanceBasis
	Dispute         *DisputeSummary
	PairDisputes    []DisputeSummary
	CleanupErrors   []string
	Attempts        []AttemptSummary
	PairGenerations uint64
	Replacements    uint64
	Reputation      map[string]Reputation
}

type replica struct {
	endpoint   Endpoint
	commitment Commitment
	record     int
}

type activePair struct {
	replicas   [2]replica
	generation uint64
}

func (pair activePair) commitments() []Commitment {
	return []Commitment{pair.replicas[0].commitment, pair.replicas[1].commitment}
}

type roundClock struct{ next uint64 }

func (clock *roundClock) take() uint64 {
	round := clock.next
	clock.next++
	return round
}

type unavailableError struct {
	slots []int
	phase string
	err   error
}

func (failure *unavailableError) Error() string {
	return fmt.Sprintf("%s unavailable in slots %v: %v", failure.phase, failure.slots, failure.err)
}

func (failure *unavailableError) Unwrap() error { return failure.err }

// Coordinator keeps two active replicas from an ordered worker pool.
type Coordinator struct {
	endpoints      []Endpoint
	client         *http.Client
	limits         protocol.Limits
	requestTimeout time.Duration
	jobTimeout     time.Duration
	requestRetries int
	verifier       Verifier
	journal        *wal.Log
	attempts       atomic.Uint64
	reputation     reputationLog
}

// New validates an ordered loopback worker pool.
func New(endpoints []Endpoint, client *http.Client, limits protocol.Limits) (*Coordinator, error) {
	config := DefaultConfig()
	config.Limits = limits
	return NewWithConfig(endpoints, client, config)
}

// NewWithConfig validates an ordered loopback worker pool and deadline policy.
func NewWithConfig(endpoints []Endpoint, client *http.Client, config Config) (*Coordinator, error) {
	if len(endpoints) < 2 {
		return nil, fmt.Errorf("coordinator requires at least two workers, got %d", len(endpoints))
	}
	if client == nil {
		return nil, fmt.Errorf("HTTP client is required")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	validated := make([]Endpoint, len(endpoints))
	workerIDs := make(map[string]struct{}, len(endpoints))
	for i, endpoint := range endpoints {
		parsed, err := validateEndpoint(endpoint)
		if err != nil {
			return nil, fmt.Errorf("worker %d: %w", i, err)
		}
		validated[i] = parsed
		if _, exists := workerIDs[parsed.ID]; exists {
			return nil, fmt.Errorf("worker IDs must be distinct")
		}
		workerIDs[parsed.ID] = struct{}{}
	}
	ownedClient := *client
	ownedClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	coordinator := &Coordinator{
		endpoints:      validated,
		client:         &ownedClient,
		limits:         config.Limits,
		requestTimeout: config.RequestTimeout,
		jobTimeout:     config.JobTimeout,
		requestRetries: config.RequestRetries,
		verifier:       config.Verifier,
	}
	if coordinator.verifier == nil {
		coordinator.verifier = StrictVerifier{}
	}
	if config.WALPath != "" {
		journal, err := wal.Open(config.WALPath)
		if err != nil {
			return nil, err
		}
		coordinator.journal = journal
	}
	return coordinator, nil
}

// Close releases the coordinator's durable journal. It is safe to call when
// journaling is disabled.
func (coordinator *Coordinator) Close() error {
	if coordinator.journal == nil {
		return nil
	}
	return coordinator.journal.Close()
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

// Run dispatches a job to an active pair and deterministically replaces
// workers that fail to answer while the coordinator-owned job deadline holds.
func (coordinator *Coordinator) Run(ctx context.Context, spec job.JobSpec) (Result, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, coordinator.jobTimeout, errJobDeadline)
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
	if err := coordinator.recordTransition(jobID, "job/"+protocol.EncodeDigest(jobID)+"/start", "job_started", durableJobStart{Job: encodedJob}); err != nil {
		return Result{}, fmt.Errorf("durably record job start: %w", err)
	}
	run := jobRun{
		coordinator: coordinator,
		ctx:         ctx,
		machine:     machine,
		encodedJob:  encodedJob,
		jobID:       jobID,
		clock:       roundClock{next: 1},
	}
	result, runErr := run.execute()
	if runErr != nil {
		return result, runErr
	}
	if err := coordinator.recordTerminalWithMachine(machine, result); err != nil {
		return result, fmt.Errorf("durably record terminal result: %w", err)
	}
	return result, nil
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
	if coordinator.verifier != nil {
		if _, err := coordinator.verifier.Verify(ctx, machine, []Candidate{{
			WorkerID: endpoint.ID, State: state, Hash: wantHash,
		}}); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return job.State{}, false, err
			}
			return job.State{}, true, fmt.Errorf("strict verification failed: %w", err)
		}
	}
	return state, false, nil
}

func (coordinator *Coordinator) closePair(
	ctx context.Context,
	jobID protocol.Digest,
	pair activePair,
	round uint64,
) []string {
	errorsByWorker := make([]error, len(pair.replicas))
	var group sync.WaitGroup
	group.Add(len(pair.replicas))
	for i, replica := range pair.replicas {
		i, replica := i, replica
		go func() {
			defer group.Done()
			errorsByWorker[i] = coordinator.closeReplica(ctx, jobID, replica, round)
		}()
	}
	group.Wait()
	var messages []string
	for i, err := range errorsByWorker {
		if err != nil {
			messages = append(messages, fmt.Sprintf("%s: %v", pair.replicas[i].endpoint.ID, err))
		}
	}
	return messages
}

func (coordinator *Coordinator) closeReplica(
	ctx context.Context,
	jobID protocol.Digest,
	replica replica,
	round uint64,
) error {
	meta := protocol.Meta{
		JobID: protocol.EncodeDigest(jobID), AttemptID: replica.commitment.AttemptID, Round: round,
	}
	var response protocol.CloseResponse
	if err := coordinator.post(
		ctx, replica.endpoint.URL+protocol.ClosePath, protocol.CloseRequest{Meta: meta}, &response,
	); err != nil {
		return err
	}
	if !protocol.SameMeta(response.Meta, meta) || response.WorkerID != replica.endpoint.ID || !response.Closed {
		return fmt.Errorf("invalid close acknowledgement")
	}
	return nil
}

func (coordinator *Coordinator) post(ctx context.Context, address string, requestValue, responseValue any) error {
	var lastErr error
	for attempt := 0; attempt <= coordinator.requestRetries; attempt++ {
		lastErr = coordinator.postOnce(ctx, address, requestValue, responseValue)
		if lastErr == nil {
			return nil
		}
		if !errors.Is(lastErr, errWorkerUnavailable) || context.Cause(ctx) != nil {
			return lastErr
		}
	}
	return lastErr
}

func (coordinator *Coordinator) postOnce(ctx context.Context, address string, requestValue, responseValue any) error {
	ctx, cancel := context.WithTimeoutCause(ctx, coordinator.requestTimeout, errRequestDeadline)
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
		if cause := context.Cause(ctx); cause != nil {
			if errors.Is(cause, errRequestDeadline) {
				return fmt.Errorf("%w: %w", errWorkerUnavailable, cause)
			}
			return cause
		}
		return fmt.Errorf("%w: %v", errWorkerUnavailable, err)
	}
	defer response.Body.Close()

	limited := io.LimitReader(response.Body, coordinator.limits.MaxBodyBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			if errors.Is(cause, errRequestDeadline) {
				return fmt.Errorf("%w: %w", errWorkerUnavailable, cause)
			}
			return cause
		}
		return fmt.Errorf("%w: read response: %v", errWorkerUnavailable, err)
	}
	if int64(len(data)) > coordinator.limits.MaxBodyBytes {
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			if isUnavailableStatus(response.StatusCode) {
				return fmt.Errorf("%w: oversized HTTP %d response", errWorkerUnavailable, response.StatusCode)
			}
			return fmt.Errorf("worker returned oversized HTTP %d response", response.StatusCode)
		}
		return fmt.Errorf("%w: body exceeds %d-byte protocol limit", errInvalidWorkerResponse, coordinator.limits.MaxBodyBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var failure protocol.ErrorResponse
		if isUnavailableStatus(response.StatusCode) {
			if err := decodeOne(data, &failure); err == nil {
				return fmt.Errorf("%w: HTTP %d %s: %s", errWorkerUnavailable, response.StatusCode, failure.Code, failure.Message)
			}
			return fmt.Errorf("%w: HTTP %d", errWorkerUnavailable, response.StatusCode)
		}
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

func isUnavailableStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return status >= 500
	}
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
