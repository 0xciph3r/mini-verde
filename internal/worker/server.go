// Package worker executes canonical jobs and exposes their commitments over
// the Mini-Verde HTTP protocol.
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/0xciph3r/mini-verde/internal/job"
	"github.com/0xciph3r/mini-verde/internal/merkle"
	"github.com/0xciph3r/mini-verde/internal/protocol"
)

type attemptStatus uint8

const (
	attemptRunning attemptStatus = iota + 1
	attemptReady
	attemptClosed
)

type attemptKey struct {
	jobID     protocol.Digest
	attemptID uint64
}

type attempt struct {
	status        attemptStatus
	highestRound  uint64
	lastOperation string
	lastRequest   [sha256.Size]byte
	done          chan struct{}
	err           error
	commitment    protocol.ExecuteResponse
	stateHashes   []protocol.Digest
	tree          *merkle.Tree
	machine       *job.Machine
	checkpoints   map[uint64]job.State
	finalState    []byte
}

// Server is a concurrency-safe HTTP worker.
type Server struct {
	id             string
	limits         protocol.Limits
	mux            *http.ServeMux
	jobs           chan struct{}
	mu             sync.Mutex
	attempts       map[attemptKey]*attempt
	executions     atomic.Uint64
	recomputations atomic.Uint64
}

func New(id string, limits protocol.Limits) (*Server, error) {
	if strings.TrimSpace(id) == "" || len(id) > 128 {
		return nil, fmt.Errorf("worker ID must contain 1 to 128 characters")
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	server := &Server{
		id:       id,
		limits:   limits,
		mux:      http.NewServeMux(),
		jobs:     make(chan struct{}, limits.MaxConcurrentExecutions),
		attempts: make(map[attemptKey]*attempt),
	}
	server.mux.HandleFunc(protocol.ExecutePath, server.handleExecute)
	server.mux.HandleFunc(protocol.FinalStatePath, server.handleFinalState)
	server.mux.HandleFunc(protocol.StepHashPath, server.handleStepHash)
	server.mux.HandleFunc(protocol.StatePath, server.handleState)
	server.mux.HandleFunc(protocol.ClosePath, server.handleClose)
	return server, nil
}

func (server *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	server.mux.ServeHTTP(response, request)
}

func (server *Server) ActiveAttempts() int {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.activeAttemptsLocked()
}

func (server *Server) ClosedAttempts() int {
	server.mu.Lock()
	defer server.mu.Unlock()
	var count int
	for _, current := range server.attempts {
		if current.status == attemptClosed {
			count++
		}
	}
	return count
}

// ExecutionCount returns the number of attempt owners that entered canonical
// execution. Retries that join a running or ready attempt do not increment it.
func (server *Server) ExecutionCount() uint64 {
	return server.executions.Load()
}

// RecomputeCount returns the actual number of canonical steps executed while
// serving indexed-state requests.
func (server *Server) RecomputeCount() uint64 {
	return server.recomputations.Load()
}

func (server *Server) handleExecute(response http.ResponseWriter, request *http.Request) {
	var message protocol.ExecuteRequest
	fingerprint, ok := server.decode(response, request, &message)
	if !ok {
		return
	}
	jobID, err := protocol.ValidateMeta(message.Meta)
	if err != nil {
		server.writeError(response, http.StatusBadRequest, message.Meta, "bad_meta", err)
		return
	}
	if message.Round != 0 {
		server.writeError(response, http.StatusBadRequest, message.Meta, "bad_meta", fmt.Errorf("execute requires round 0"))
		return
	}
	spec, err := job.UnmarshalSpec(message.Job)
	if err != nil {
		server.writeError(response, http.StatusBadRequest, message.Meta, "bad_job", err)
		return
	}
	if err := server.limits.ValidateJob(spec); err != nil {
		server.writeError(response, http.StatusUnprocessableEntity, message.Meta, "job_rejected", err)
		return
	}
	machine, err := job.NewMachine(spec)
	if err != nil {
		server.writeError(response, http.StatusBadRequest, message.Meta, "bad_job", err)
		return
	}
	if machine.ID() != jobID {
		server.writeError(response, http.StatusConflict, message.Meta, "job_id_mismatch", fmt.Errorf("canonical job ID does not match envelope"))
		return
	}

	key := attemptKey{jobID: jobID, attemptID: message.AttemptID}
	current, owner, status, err := server.beginAttempt(key, fingerprint)
	if err != nil {
		server.writeError(response, status, message.Meta, "attempt_rejected", err)
		return
	}
	if !owner {
		select {
		case <-current.done:
		case <-request.Context().Done():
			server.writeError(response, http.StatusRequestTimeout, message.Meta, "request_cancelled", request.Context().Err())
			return
		}
		server.mu.Lock()
		commitment, attemptErr, attemptStatus := current.commitment, current.err, current.status
		server.mu.Unlock()
		if attemptErr != nil || attemptStatus != attemptReady {
			server.writeError(response, http.StatusInternalServerError, message.Meta, "execution_failed", attemptErr)
			return
		}
		server.writeJSON(response, http.StatusOK, commitment)
		return
	}

	// The attempt outlives this RPC. A coordinator deadline stops waiting but
	// does not erase bounded work that an exact retry can later join.
	executionContext := context.WithoutCancel(request.Context())
	commitment, hashes, tree, checkpoints, finalState, err := server.execute(executionContext, machine, message.Meta)
	server.finishAttempt(key, machine, commitment, hashes, tree, checkpoints, finalState, err)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusRequestTimeout
		}
		server.writeError(response, status, message.Meta, "execution_failed", err)
		return
	}
	server.writeJSON(response, http.StatusOK, commitment)
}

func (server *Server) beginAttempt(key attemptKey, fingerprint [sha256.Size]byte) (*attempt, bool, int, error) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if current, exists := server.attempts[key]; exists {
		if current.status == attemptClosed {
			return nil, false, http.StatusConflict, fmt.Errorf("attempt is closed")
		}
		if current.highestRound != 0 || current.lastOperation != "execute" {
			return nil, false, http.StatusConflict, fmt.Errorf("execute round is stale")
		}
		if current.lastRequest != fingerprint {
			return nil, false, http.StatusConflict, fmt.Errorf("execute retry bytes differ from the original request")
		}
		return current, false, 0, nil
	}
	if server.activeAttemptsLocked() >= server.limits.MaxActiveAttempts {
		return nil, false, http.StatusServiceUnavailable, fmt.Errorf("worker retains the maximum number of active attempts")
	}
	current := &attempt{
		status: attemptRunning, done: make(chan struct{}), lastOperation: "execute", lastRequest: fingerprint,
	}
	server.attempts[key] = current
	return current, true, 0, nil
}

func (server *Server) finishAttempt(
	key attemptKey,
	machine *job.Machine,
	commitment protocol.ExecuteResponse,
	hashes []protocol.Digest,
	tree *merkle.Tree,
	checkpoints map[uint64]job.State,
	finalState []byte,
	err error,
) {
	server.mu.Lock()
	defer server.mu.Unlock()
	current := server.attempts[key]
	current.err = err
	if err == nil {
		current.status = attemptReady
		current.commitment = commitment
		current.stateHashes = hashes
		current.tree = tree
		current.machine = machine
		current.checkpoints = checkpoints
		current.finalState = finalState
	} else {
		delete(server.attempts, key)
	}
	close(current.done)
}

func (server *Server) execute(
	ctx context.Context,
	machine *job.Machine,
	meta protocol.Meta,
) (protocol.ExecuteResponse, []protocol.Digest, *merkle.Tree, map[uint64]job.State, []byte, error) {
	server.executions.Add(1)
	select {
	case server.jobs <- struct{}{}:
		defer func() { <-server.jobs }()
	case <-ctx.Done():
		return protocol.ExecuteResponse{}, nil, nil, nil, nil, ctx.Err()
	}

	state := machine.InitialState()
	checkpoints := map[uint64]job.State{0: state}
	hashes := make([]protocol.Digest, 0, machine.LeafCount())
	for {
		select {
		case <-ctx.Done():
			return protocol.ExecuteResponse{}, nil, nil, nil, nil, ctx.Err()
		default:
		}
		stateHash, err := machine.HashState(state)
		if err != nil {
			return protocol.ExecuteResponse{}, nil, nil, nil, nil, err
		}
		hashes = append(hashes, stateHash)
		if uint64(len(hashes)) == machine.LeafCount() {
			break
		}
		state, err = machine.Step(state)
		if err != nil {
			return protocol.ExecuteResponse{}, nil, nil, nil, nil, err
		}
		if state.Step%protocol.CheckpointInterval == 0 {
			checkpoints[state.Step] = state
		}
	}
	tree, err := merkle.New(machine.ID(), machine.LeafCount(), hashes)
	if err != nil {
		return protocol.ExecuteResponse{}, nil, nil, nil, nil, err
	}
	finalState, err := machine.MarshalState(state)
	if err != nil {
		return protocol.ExecuteResponse{}, nil, nil, nil, nil, err
	}
	commitment := protocol.ExecuteResponse{
		Meta:           meta,
		WorkerID:       server.id,
		Root:           protocol.EncodeDigest(tree.Root()),
		FinalStateHash: protocol.EncodeDigest(hashes[len(hashes)-1]),
	}
	return commitment, hashes, tree, checkpoints, finalState, nil
}

func (server *Server) handleStepHash(response http.ResponseWriter, request *http.Request) {
	var message protocol.StepHashRequest
	fingerprint, ok := server.decode(response, request, &message)
	if !ok {
		return
	}
	jobID, err := protocol.ValidateMeta(message.Meta)
	if err != nil {
		server.writeError(response, http.StatusBadRequest, message.Meta, "bad_meta", err)
		return
	}
	key := attemptKey{jobID: jobID, attemptID: message.AttemptID}
	server.mu.Lock()
	current, exists := server.attempts[key]
	if !exists {
		server.mu.Unlock()
		server.writeError(response, http.StatusNotFound, message.Meta, "unknown_attempt", fmt.Errorf("attempt does not exist"))
		return
	}
	if current.status != attemptReady {
		server.mu.Unlock()
		server.writeError(response, http.StatusConflict, message.Meta, "attempt_not_ready", fmt.Errorf("attempt is not ready"))
		return
	}
	if message.Index >= uint64(len(current.stateHashes)) {
		server.mu.Unlock()
		server.writeError(response, http.StatusBadRequest, message.Meta, "bad_index", fmt.Errorf("state index is outside the trace"))
		return
	}
	if err := advanceRound(current, message.Round, "step-hash", fingerprint); err != nil {
		server.mu.Unlock()
		server.writeError(response, http.StatusConflict, message.Meta, "stale_round", err)
		return
	}
	stateHash := current.stateHashes[message.Index]
	proof, err := current.tree.Proof(message.Index)
	server.mu.Unlock()
	if err != nil {
		server.writeError(response, http.StatusInternalServerError, message.Meta, "proof_failed", err)
		return
	}
	siblings := make([]string, len(proof.Siblings))
	for i, sibling := range proof.Siblings {
		siblings[i] = protocol.EncodeDigest(sibling)
	}
	server.writeJSON(response, http.StatusOK, protocol.StepHashResponse{
		Meta: message.Meta, WorkerID: server.id, Hash: protocol.EncodeDigest(stateHash),
		Proof: protocol.InclusionProof{Index: proof.Index, Siblings: siblings},
	})
}

func (server *Server) handleState(response http.ResponseWriter, request *http.Request) {
	var message protocol.StateRequest
	fingerprint, ok := server.decode(response, request, &message)
	if !ok {
		return
	}
	jobID, err := protocol.ValidateMeta(message.Meta)
	if err != nil {
		server.writeError(response, http.StatusBadRequest, message.Meta, "bad_meta", err)
		return
	}
	key := attemptKey{jobID: jobID, attemptID: message.AttemptID}
	server.mu.Lock()
	current, exists := server.attempts[key]
	if !exists {
		server.mu.Unlock()
		server.writeError(response, http.StatusNotFound, message.Meta, "unknown_attempt", fmt.Errorf("attempt does not exist"))
		return
	}
	if current.status != attemptReady {
		server.mu.Unlock()
		server.writeError(response, http.StatusConflict, message.Meta, "attempt_not_ready", fmt.Errorf("attempt is not ready"))
		return
	}
	if message.Index >= uint64(len(current.stateHashes)) {
		server.mu.Unlock()
		server.writeError(response, http.StatusBadRequest, message.Meta, "bad_index", fmt.Errorf("state index is outside the trace"))
		return
	}
	if err := advanceRound(current, message.Round, "state", fingerprint); err != nil {
		server.mu.Unlock()
		server.writeError(response, http.StatusConflict, message.Meta, "stale_round", err)
		return
	}
	checkpointIndex := message.Index / protocol.CheckpointInterval * protocol.CheckpointInterval
	state := current.checkpoints[checkpointIndex]
	machine := current.machine
	server.mu.Unlock()

	select {
	case server.jobs <- struct{}{}:
		defer func() { <-server.jobs }()
	case <-request.Context().Done():
		server.writeError(response, http.StatusRequestTimeout, message.Meta, "request_cancelled", request.Context().Err())
		return
	}
	for state.Step < message.Index {
		select {
		case <-request.Context().Done():
			server.writeError(response, http.StatusRequestTimeout, message.Meta, "request_cancelled", request.Context().Err())
			return
		default:
		}
		state, err = machine.Step(state)
		if err != nil {
			server.writeError(response, http.StatusInternalServerError, message.Meta, "recompute_failed", err)
			return
		}
		server.recomputations.Add(1)
	}
	encoded, err := machine.MarshalState(state)
	if err != nil {
		server.writeError(response, http.StatusInternalServerError, message.Meta, "state_failed", err)
		return
	}
	server.writeJSON(response, http.StatusOK, protocol.StateResponse{
		Meta: message.Meta, WorkerID: server.id, Index: message.Index, State: encoded,
	})
}

func (server *Server) handleFinalState(response http.ResponseWriter, request *http.Request) {
	var message protocol.FinalStateRequest
	fingerprint, ok := server.decode(response, request, &message)
	if !ok {
		return
	}
	jobID, err := protocol.ValidateMeta(message.Meta)
	if err != nil {
		server.writeError(response, http.StatusBadRequest, message.Meta, "bad_meta", err)
		return
	}
	key := attemptKey{jobID: jobID, attemptID: message.AttemptID}
	server.mu.Lock()
	current, exists := server.attempts[key]
	if !exists {
		server.mu.Unlock()
		server.writeError(response, http.StatusNotFound, message.Meta, "unknown_attempt", fmt.Errorf("attempt does not exist"))
		return
	}
	if current.status != attemptReady {
		server.mu.Unlock()
		server.writeError(response, http.StatusConflict, message.Meta, "attempt_not_ready", fmt.Errorf("attempt is not ready"))
		return
	}
	if err := advanceRound(current, message.Round, "final-state", fingerprint); err != nil {
		server.mu.Unlock()
		server.writeError(response, http.StatusConflict, message.Meta, "stale_round", err)
		return
	}
	state := append([]byte(nil), current.finalState...)
	server.mu.Unlock()
	server.writeJSON(response, http.StatusOK, protocol.FinalStateResponse{
		Meta: message.Meta, WorkerID: server.id, State: state,
	})
}

func (server *Server) handleClose(response http.ResponseWriter, request *http.Request) {
	var message protocol.CloseRequest
	fingerprint, ok := server.decode(response, request, &message)
	if !ok {
		return
	}
	jobID, err := protocol.ValidateMeta(message.Meta)
	if err != nil {
		server.writeError(response, http.StatusBadRequest, message.Meta, "bad_meta", err)
		return
	}
	key := attemptKey{jobID: jobID, attemptID: message.AttemptID}
	server.mu.Lock()
	current, exists := server.attempts[key]
	if !exists {
		server.mu.Unlock()
		server.writeError(response, http.StatusNotFound, message.Meta, "unknown_attempt", fmt.Errorf("attempt does not exist"))
		return
	}
	if current.status == attemptClosed && current.highestRound == message.Round && current.lastOperation == "close" && current.lastRequest == fingerprint {
		server.mu.Unlock()
		server.writeJSON(response, http.StatusOK, protocol.CloseResponse{Meta: message.Meta, WorkerID: server.id, Closed: true})
		return
	}
	if current.status != attemptReady {
		server.mu.Unlock()
		server.writeError(response, http.StatusConflict, message.Meta, "attempt_not_ready", fmt.Errorf("attempt cannot be closed"))
		return
	}
	if err := advanceRound(current, message.Round, "close", fingerprint); err != nil {
		server.mu.Unlock()
		server.writeError(response, http.StatusConflict, message.Meta, "stale_round", err)
		return
	}
	current.status = attemptClosed
	current.stateHashes = nil
	current.tree = nil
	current.machine = nil
	current.checkpoints = nil
	current.finalState = nil
	server.mu.Unlock()
	server.writeJSON(response, http.StatusOK, protocol.CloseResponse{Meta: message.Meta, WorkerID: server.id, Closed: true})
}

func advanceRound(current *attempt, round uint64, operation string, fingerprint [sha256.Size]byte) error {
	if round < current.highestRound {
		return fmt.Errorf("round %d is older than %d", round, current.highestRound)
	}
	if round == current.highestRound {
		if current.lastOperation == operation && current.lastRequest == fingerprint {
			return nil
		}
		return fmt.Errorf("round %d was already used by a different request", round)
	}
	current.highestRound = round
	current.lastOperation = operation
	current.lastRequest = fingerprint
	return nil
}

func (server *Server) activeAttemptsLocked() int {
	var count int
	for _, current := range server.attempts {
		if current.status != attemptClosed {
			count++
		}
	}
	return count
}

func (server *Server) decode(response http.ResponseWriter, request *http.Request, destination any) ([sha256.Size]byte, bool) {
	var zero [sha256.Size]byte
	if request.Method != http.MethodPost {
		response.Header().Set("Allow", http.MethodPost)
		server.writeError(response, http.StatusMethodNotAllowed, protocol.Meta{}, "method_not_allowed", fmt.Errorf("POST required"))
		return zero, false
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != protocol.JSONContentType {
		server.writeError(response, http.StatusUnsupportedMediaType, protocol.Meta{}, "unsupported_media_type", fmt.Errorf("Content-Type must be application/json"))
		return zero, false
	}
	if request.ContentLength > server.limits.MaxBodyBytes {
		server.writeError(response, http.StatusRequestEntityTooLarge, protocol.Meta{}, "body_too_large", fmt.Errorf("request body exceeds %d bytes", server.limits.MaxBodyBytes))
		return zero, false
	}
	request.Body = http.MaxBytesReader(response, request.Body, server.limits.MaxBodyBytes)
	body, err := io.ReadAll(request.Body)
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			server.writeError(response, http.StatusRequestEntityTooLarge, protocol.Meta{}, "body_too_large", err)
			return zero, false
		}
		server.writeError(response, http.StatusBadRequest, protocol.Meta{}, "bad_body", err)
		return zero, false
	}
	fingerprint := fingerprintRequest(request, body)
	decoder := jsonNewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(destination); err != nil {
		server.writeError(response, http.StatusBadRequest, protocol.Meta{}, "bad_json", err)
		return zero, false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		server.writeError(response, http.StatusBadRequest, protocol.Meta{}, "bad_json", fmt.Errorf("request must contain one JSON value"))
		return zero, false
	}
	return fingerprint, true
}

func fingerprintRequest(request *http.Request, body []byte) [sha256.Size]byte {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(request.Method))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write([]byte(request.URL.Path))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write(body)
	var fingerprint [sha256.Size]byte
	copy(fingerprint[:], hasher.Sum(nil))
	return fingerprint
}

func (server *Server) writeError(response http.ResponseWriter, status int, meta protocol.Meta, code string, err error) {
	message := "request failed"
	if err != nil {
		message = err.Error()
	}
	server.writeJSON(response, status, protocol.ErrorResponse{Meta: meta, Code: code, Message: message})
}

func (server *Server) writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", protocol.JSONContentType)
	response.Header().Set("X-Content-Type-Options", "nosniff")
	response.WriteHeader(status)
	_ = jsonNewEncoder(response).Encode(value)
}
