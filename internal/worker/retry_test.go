package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/0xciph3r/mini-verde/internal/job"
	"github.com/0xciph3r/mini-verde/internal/protocol"
)

func TestDuplicateExecuteWhileRunningExecutesOnce(t *testing.T) {
	limits := protocol.DefaultLimits()
	limits.MaxConcurrentExecutions = 1
	server, err := New("worker-a", limits)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	server.jobs <- struct{}{}
	payload := executePayload(t, 1)

	first := make(chan *httptest.ResponseRecorder, 1)
	go func() { first <- servePayload(server, protocol.ExecutePath, payload) }()
	waitFor(t, func() bool { return server.ExecutionCount() == 1 })

	duplicate := make(chan *httptest.ResponseRecorder, 1)
	go func() { duplicate <- servePayload(server, protocol.ExecutePath, payload) }()

	different := append(bytes.Clone(payload), '\n')
	if response := servePayload(server, protocol.ExecutePath, different); response.Code != http.StatusConflict {
		t.Fatalf("different-byte retry status = %d, want %d", response.Code, http.StatusConflict)
	}
	<-server.jobs
	firstResponse := <-first
	duplicateResponse := <-duplicate
	if firstResponse.Code != http.StatusOK || duplicateResponse.Code != http.StatusOK {
		t.Fatalf("execute statuses = %d, %d; want 200, 200", firstResponse.Code, duplicateResponse.Code)
	}
	if !bytes.Equal(firstResponse.Body.Bytes(), duplicateResponse.Body.Bytes()) {
		t.Fatal("duplicate execute did not receive the cached response")
	}
	if got := server.ExecutionCount(); got != 1 {
		t.Fatalf("execution count = %d, want 1", got)
	}
}

func TestExecuteAttemptOutlivesRPCDeadlineAndRemainsRetryable(t *testing.T) {
	limits := protocol.DefaultLimits()
	limits.MaxConcurrentExecutions = 1
	server, err := New("worker-a", limits)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	server.jobs <- struct{}{}
	payload := executePayload(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, protocol.ExecutePath, bytes.NewReader(payload)).WithContext(ctx)
	request.Header.Set("Content-Type", protocol.JSONContentType)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		server.ServeHTTP(response, request)
		close(done)
	}()
	waitFor(t, func() bool { return server.ExecutionCount() == 1 })
	cancel()
	select {
	case <-done:
		t.Fatal("request cancellation erased the running attempt")
	case <-time.After(10 * time.Millisecond):
	}
	if got := server.ActiveAttempts(); got != 1 {
		t.Fatalf("active attempts after cancellation = %d, want 1", got)
	}
	<-server.jobs
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("detached attempt did not finish")
	}
	if retry := servePayload(server, protocol.ExecutePath, payload); retry.Code != http.StatusOK {
		t.Fatalf("retry status = %d, want %d", retry.Code, http.StatusOK)
	}
	if got := server.ExecutionCount(); got != 1 {
		t.Fatalf("execution count = %d, want 1", got)
	}
}

func TestCloseCancelsAndReclaimsRunningAttempt(t *testing.T) {
	limits := protocol.DefaultLimits()
	limits.MaxConcurrentExecutions = 1
	server, err := New("worker-a", limits)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	server.jobs <- struct{}{}
	defer func() { <-server.jobs }()
	execute := executePayload(t, 1)
	executeDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { executeDone <- servePayload(server, protocol.ExecutePath, execute) }()
	waitFor(t, func() bool { return server.ExecutionCount() == 1 })

	meta := payloadMeta(t, execute)
	meta.Round = 1
	closePayload := marshalPayload(t, protocol.CloseRequest{Meta: meta})
	closed := servePayload(server, protocol.ClosePath, closePayload)
	if closed.Code != http.StatusOK {
		t.Fatalf("close status = %d, body = %s", closed.Code, closed.Body.String())
	}
	select {
	case <-executeDone:
	case <-time.After(time.Second):
		t.Fatal("cancelled execution did not return")
	}
	if got := server.ActiveAttempts(); got != 0 {
		t.Fatalf("active attempts = %d, want 0", got)
	}
	if got := server.ClosedAttempts(); got != 1 {
		t.Fatalf("closed attempts = %d, want 1", got)
	}
	if retry := servePayload(server, protocol.ClosePath, closePayload); retry.Code != http.StatusOK {
		t.Fatalf("idempotent close status = %d, want %d", retry.Code, http.StatusOK)
	}
}

func TestRoundRuleMatrix(t *testing.T) {
	server, err := New("worker-a", protocol.DefaultLimits())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	execute := executePayload(t, 1)
	if response := servePayload(server, protocol.ExecutePath, execute); response.Code != http.StatusOK {
		t.Fatalf("execute status = %d", response.Code)
	}
	if response := servePayload(server, protocol.ExecutePath, execute); response.Code != http.StatusOK {
		t.Fatalf("ready execute retry status = %d", response.Code)
	}

	meta := payloadMeta(t, execute)
	meta.Round = 2 // A worker may skip a job-wide round it did not participate in.
	statePayload := marshalPayload(t, protocol.FinalStateRequest{Meta: meta})
	if response := servePayload(server, protocol.FinalStatePath, statePayload); response.Code != http.StatusOK {
		t.Fatalf("skipped-round status = %d", response.Code)
	}
	if response := servePayload(server, protocol.FinalStatePath, statePayload); response.Code != http.StatusOK {
		t.Fatalf("exact retry status = %d", response.Code)
	}
	if response := servePayload(server, protocol.FinalStatePath, append(bytes.Clone(statePayload), ' ')); response.Code != http.StatusConflict {
		t.Fatalf("different-byte retry status = %d, want %d", response.Code, http.StatusConflict)
	}
	if response := servePayload(server, protocol.ClosePath, statePayload); response.Code != http.StatusConflict {
		t.Fatalf("same-round different-operation status = %d, want %d", response.Code, http.StatusConflict)
	}

	meta.Round = 1
	stalePayload := marshalPayload(t, protocol.FinalStateRequest{Meta: meta})
	if response := servePayload(server, protocol.FinalStatePath, stalePayload); response.Code != http.StatusConflict {
		t.Fatalf("stale status = %d, want %d", response.Code, http.StatusConflict)
	}

	meta.Round = 4
	closePayload := marshalPayload(t, protocol.CloseRequest{Meta: meta})
	if response := servePayload(server, protocol.ClosePath, closePayload); response.Code != http.StatusOK {
		t.Fatalf("close status = %d", response.Code)
	}
	if response := servePayload(server, protocol.ClosePath, closePayload); response.Code != http.StatusOK {
		t.Fatalf("exact close retry status = %d", response.Code)
	}
	if response := servePayload(server, protocol.ClosePath, append(bytes.Clone(closePayload), '\n')); response.Code != http.StatusConflict {
		t.Fatalf("different-byte close retry status = %d, want %d", response.Code, http.StatusConflict)
	}
}

func TestEqualRoundWithDifferentStepHashIndexIsRejected(t *testing.T) {
	server, err := New("worker-a", protocol.DefaultLimits())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	execute := executePayload(t, 1)
	if response := servePayload(server, protocol.ExecutePath, execute); response.Code != http.StatusOK {
		t.Fatalf("execute status = %d", response.Code)
	}
	meta := payloadMeta(t, execute)
	meta.Round = 1
	indexZero := marshalPayload(t, protocol.StepHashRequest{Meta: meta, Index: 0})
	if response := servePayload(server, protocol.StepHashPath, indexZero); response.Code != http.StatusOK {
		t.Fatalf("step-hash status = %d", response.Code)
	}
	if response := servePayload(server, protocol.StepHashPath, indexZero); response.Code != http.StatusOK {
		t.Fatalf("exact step-hash retry status = %d", response.Code)
	}
	indexOne := marshalPayload(t, protocol.StepHashRequest{Meta: meta, Index: 1})
	if response := servePayload(server, protocol.StepHashPath, indexOne); response.Code != http.StatusConflict {
		t.Fatalf("same-round different-index status = %d, want %d", response.Code, http.StatusConflict)
	}
}

func executePayload(t *testing.T, attemptID uint64) []byte {
	t.Helper()
	spec := job.JobSpec{
		Version: job.ProtocolVersion, Seed: 1, Steps: 1,
		InputSize: 1, HiddenSize: 1, OutputSize: 1,
		BatchSize: 1, LearningRate: job.DefaultLearningRate,
		Dataset: job.Dataset{Examples: 1, Inputs: []float32{0.5}, Targets: []float32{0.25}},
	}
	machine, err := job.NewMachine(spec)
	if err != nil {
		t.Fatalf("NewMachine() error = %v", err)
	}
	encoded, err := job.MarshalSpec(spec)
	if err != nil {
		t.Fatalf("MarshalSpec() error = %v", err)
	}
	return marshalPayload(t, protocol.ExecuteRequest{
		Meta: protocol.Meta{JobID: protocol.EncodeDigest(machine.ID()), AttemptID: attemptID, Round: 0},
		Job:  encoded,
	})
}

func payloadMeta(t *testing.T, payload []byte) protocol.Meta {
	t.Helper()
	var request protocol.ExecuteRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	return request.Meta
}

func marshalPayload(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return payload
}

func servePayload(handler http.Handler, path string, payload []byte) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	request.Header.Set("Content-Type", protocol.JSONContentType)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not met before deadline")
		}
		time.Sleep(time.Millisecond)
	}
}
