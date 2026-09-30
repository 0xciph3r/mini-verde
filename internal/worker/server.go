// Package worker executes canonical jobs and exposes their commitments over
// the Mini-Verde HTTP protocol.
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"

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
	done          chan struct{}
	err           error
	commitment    protocol.ExecuteResponse
	stateHashes   []protocol.Digest
	tree          *merkle.Tree
	finalState    []byte
}

// Server is a concurrency-safe HTTP worker.
type Server struct {
	id       string
	limits   protocol.Limits
	mux      *http.ServeMux
	jobs     chan struct{}
	mu       sync.Mutex
	attempts map[attemptKey]*attempt
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

func (server *Server) handleExecute(response http.ResponseWriter, request *http.Request) {
	var message protocol.ExecuteRequest
	if !server.decode(response, request, &message) {
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
	current, owner, status, err := server.beginAttempt(key)
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

	commitment, hashes, tree, finalState, err := server.execute(request.Context(), machine, message.Meta)
	server.finishAttempt(key, commitment, hashes, tree, finalState, err)
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

func (server *Server) beginAttempt(key attemptKey) (*attempt, bool, int, error) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if current, exists := server.attempts[key]; exists {
		if current.status == attemptClosed {
			return nil, false, http.StatusConflict, fmt.Errorf("attempt is closed")
		}
		if current.highestRound != 0 || current.lastOperation != "execute" {
			return nil, false, http.StatusConflict, fmt.Errorf("execute round is stale")
		}
		return current, false, 0, nil
	}
	if server.activeAttemptsLocked() >= server.limits.MaxActiveAttempts {
		return nil, false, http.StatusServiceUnavailable, fmt.Errorf("worker retains the maximum number of active attempts")
	}
	current := &attempt{status: attemptRunning, done: make(chan struct{}), lastOperation: "execute"}
	server.attempts[key] = current
	return current, true, 0, nil
}

func (server *Server) finishAttempt(
	key attemptKey,
	commitment protocol.ExecuteResponse,
	hashes []protocol.Digest,
	tree *merkle.Tree,
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
) (protocol.ExecuteResponse, []protocol.Digest, *merkle.Tree, []byte, error) {
	select {
	case server.jobs <- struct{}{}:
		defer func() { <-server.jobs }()
	case <-ctx.Done():
		return protocol.ExecuteResponse{}, nil, nil, nil, ctx.Err()
	}

	state := machine.InitialState()
	hashes := make([]protocol.Digest, 0, machine.LeafCount())
	for {
		select {
		case <-ctx.Done():
			return protocol.ExecuteResponse{}, nil, nil, nil, ctx.Err()
		default:
		}
		stateHash, err := machine.HashState(state)
		if err != nil {
			return protocol.ExecuteResponse{}, nil, nil, nil, err
		}
		hashes = append(hashes, stateHash)
		if uint64(len(hashes)) == machine.LeafCount() {
			break
		}
		state, err = machine.Step(state)
		if err != nil {
			return protocol.ExecuteResponse{}, nil, nil, nil, err
		}
	}
	tree, err := merkle.New(machine.ID(), machine.LeafCount(), hashes)
	if err != nil {
		return protocol.ExecuteResponse{}, nil, nil, nil, err
	}
	finalState, err := machine.MarshalState(state)
	if err != nil {
		return protocol.ExecuteResponse{}, nil, nil, nil, err
	}
	commitment := protocol.ExecuteResponse{
		Meta:           meta,
		WorkerID:       server.id,
		Root:           protocol.EncodeDigest(tree.Root()),
		FinalStateHash: protocol.EncodeDigest(hashes[len(hashes)-1]),
	}
	return commitment, hashes, tree, finalState, nil
}

func (server *Server) handleFinalState(response http.ResponseWriter, request *http.Request) {
	var message protocol.FinalStateRequest
	if !server.decode(response, request, &message) {
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
	if err := advanceRound(current, message.Round, "final-state"); err != nil {
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
	if !server.decode(response, request, &message) {
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
	if current.status == attemptClosed && current.highestRound == message.Round && current.lastOperation == "close" {
		server.mu.Unlock()
		server.writeJSON(response, http.StatusOK, protocol.CloseResponse{Meta: message.Meta, WorkerID: server.id, Closed: true})
		return
	}
	if current.status != attemptReady {
		server.mu.Unlock()
		server.writeError(response, http.StatusConflict, message.Meta, "attempt_not_ready", fmt.Errorf("attempt cannot be closed"))
		return
	}
	if err := advanceRound(current, message.Round, "close"); err != nil {
		server.mu.Unlock()
		server.writeError(response, http.StatusConflict, message.Meta, "stale_round", err)
		return
	}
	current.status = attemptClosed
	current.stateHashes = nil
	current.tree = nil
	current.finalState = nil
	server.mu.Unlock()
	server.writeJSON(response, http.StatusOK, protocol.CloseResponse{Meta: message.Meta, WorkerID: server.id, Closed: true})
}

func advanceRound(current *attempt, round uint64, operation string) error {
	if round < current.highestRound {
		return fmt.Errorf("round %d is older than %d", round, current.highestRound)
	}
	if round == current.highestRound {
		if current.lastOperation == operation {
			return nil
		}
		return fmt.Errorf("round %d was already used for %s", round, current.lastOperation)
	}
	current.highestRound = round
	current.lastOperation = operation
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

func (server *Server) decode(response http.ResponseWriter, request *http.Request, destination any) bool {
	if request.Method != http.MethodPost {
		response.Header().Set("Allow", http.MethodPost)
		server.writeError(response, http.StatusMethodNotAllowed, protocol.Meta{}, "method_not_allowed", fmt.Errorf("POST required"))
		return false
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != protocol.JSONContentType {
		server.writeError(response, http.StatusUnsupportedMediaType, protocol.Meta{}, "unsupported_media_type", fmt.Errorf("Content-Type must be application/json"))
		return false
	}
	if request.ContentLength > server.limits.MaxBodyBytes {
		server.writeError(response, http.StatusRequestEntityTooLarge, protocol.Meta{}, "body_too_large", fmt.Errorf("request body exceeds %d bytes", server.limits.MaxBodyBytes))
		return false
	}
	request.Body = http.MaxBytesReader(response, request.Body, server.limits.MaxBodyBytes)
	decoder := jsonNewDecoder(request.Body)
	if err := decoder.Decode(destination); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			server.writeError(response, http.StatusRequestEntityTooLarge, protocol.Meta{}, "body_too_large", err)
			return false
		}
		server.writeError(response, http.StatusBadRequest, protocol.Meta{}, "bad_json", err)
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		server.writeError(response, http.StatusBadRequest, protocol.Meta{}, "bad_json", fmt.Errorf("request must contain one JSON value"))
		return false
	}
	return true
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
