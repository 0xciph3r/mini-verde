package worker_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/0xciph3r/mini-verde/internal/job"
	"github.com/0xciph3r/mini-verde/internal/merkle"
	"github.com/0xciph3r/mini-verde/internal/protocol"
	"github.com/0xciph3r/mini-verde/internal/worker"
)

func TestServerRejectsNonJSONUnknownFieldsAndOversizedBodies(t *testing.T) {
	limits := protocol.DefaultLimits()
	limits.MaxBodyBytes = 128
	server, err := worker.New("worker-a", limits)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	tests := []struct {
		name        string
		contentType string
		body        string
		wantStatus  int
	}{
		{name: "wrong content type", contentType: "text/plain", body: `{}`, wantStatus: http.StatusUnsupportedMediaType},
		{name: "unknown field", contentType: protocol.JSONContentType, body: `{"job_id":"","attempt_id":1,"round":0,"job":"","extra":true}`, wantStatus: http.StatusBadRequest},
		{name: "oversized", contentType: protocol.JSONContentType, body: `{"padding":"` + strings.Repeat("x", 256) + `"}`, wantStatus: http.StatusRequestEntityTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, protocol.ExecutePath, bytes.NewBufferString(tc.body))
			request.Header.Set("Content-Type", tc.contentType)
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)
			if response.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, tc.wantStatus, response.Body.String())
			}
		})
	}
}

func TestServerRejectsStaleExecuteAfterRoundAdvances(t *testing.T) {
	server, err := worker.New("worker-a", protocol.DefaultLimits())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
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
	meta := protocol.Meta{JobID: protocol.EncodeDigest(machine.ID()), AttemptID: 1, Round: 0}
	execute := protocol.ExecuteRequest{Meta: meta, Job: encoded}
	if status := postJSON(t, server, protocol.ExecutePath, execute); status != http.StatusOK {
		t.Fatalf("execute status = %d, want %d", status, http.StatusOK)
	}
	meta.Round = 1
	if status := postJSON(t, server, protocol.FinalStatePath, protocol.FinalStateRequest{Meta: meta}); status != http.StatusOK {
		t.Fatalf("final-state status = %d, want %d", status, http.StatusOK)
	}
	if status := postJSON(t, server, protocol.ExecutePath, execute); status != http.StatusConflict {
		t.Fatalf("stale execute status = %d, want %d", status, http.StatusConflict)
	}
}

func TestServerRefusesNinthActiveAttempt(t *testing.T) {
	limits := protocol.DefaultLimits()
	server, err := worker.New("worker-a", limits)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
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
	for attemptID := uint64(1); attemptID <= uint64(limits.MaxActiveAttempts); attemptID++ {
		request := protocol.ExecuteRequest{Meta: protocol.Meta{
			JobID: protocol.EncodeDigest(machine.ID()), AttemptID: attemptID, Round: 0,
		}, Job: encoded}
		if status := postJSON(t, server, protocol.ExecutePath, request); status != http.StatusOK {
			t.Fatalf("attempt %d status = %d, want %d", attemptID, status, http.StatusOK)
		}
	}
	request := protocol.ExecuteRequest{Meta: protocol.Meta{
		JobID: protocol.EncodeDigest(machine.ID()), AttemptID: 9, Round: 0,
	}, Job: encoded}
	if status := postJSON(t, server, protocol.ExecutePath, request); status != http.StatusServiceUnavailable {
		t.Fatalf("ninth attempt status = %d, want %d", status, http.StatusServiceUnavailable)
	}
	if got := server.ActiveAttempts(); got != 8 {
		t.Fatalf("active attempts = %d, want 8", got)
	}
}

func TestStepHashProofAndCheckpointReplay(t *testing.T) {
	server, err := worker.New("worker-a", protocol.DefaultLimits())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	spec := job.JobSpec{
		Version: job.ProtocolVersion, Seed: 7, Steps: 70,
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
	meta := protocol.Meta{JobID: protocol.EncodeDigest(machine.ID()), AttemptID: 1, Round: 0}
	var commitment protocol.ExecuteResponse
	postJSONDecode(t, server, protocol.ExecutePath, protocol.ExecuteRequest{Meta: meta, Job: encoded}, &commitment)

	meta.Round = 1
	var opening protocol.StepHashResponse
	postJSONDecode(t, server, protocol.StepHashPath, protocol.StepHashRequest{Meta: meta, Index: 70}, &opening)
	root, err := protocol.DecodeDigest(commitment.Root)
	if err != nil {
		t.Fatalf("DecodeDigest(root) error = %v", err)
	}
	stateHash, err := protocol.DecodeDigest(opening.Hash)
	if err != nil {
		t.Fatalf("DecodeDigest(hash) error = %v", err)
	}
	proof := merkle.Proof{Index: opening.Proof.Index, Siblings: make([]merkle.Digest, len(opening.Proof.Siblings))}
	for i, sibling := range opening.Proof.Siblings {
		proof.Siblings[i], err = protocol.DecodeDigest(sibling)
		if err != nil {
			t.Fatalf("DecodeDigest(sibling %d) error = %v", i, err)
		}
	}
	if !merkle.Verify(root, machine.ID(), machine.LeafCount(), stateHash, proof) {
		t.Fatal("worker returned an invalid final-leaf proof")
	}

	meta.Round = 2
	const requestedStep = uint64(63)
	var stateResponse protocol.StateResponse
	postJSONDecode(t, server, protocol.StatePath, protocol.StateRequest{Meta: meta, Index: requestedStep}, &stateResponse)
	state, err := machine.UnmarshalState(stateResponse.State)
	if err != nil {
		t.Fatalf("UnmarshalState() error = %v", err)
	}
	if state.Step != requestedStep {
		t.Fatalf("state step = %d, want %d", state.Step, requestedStep)
	}
	wantRecompute := requestedStep % protocol.CheckpointInterval
	if got := server.RecomputeCount(); got != wantRecompute {
		t.Fatalf("actual recompute steps = %d, schedule expects %d", got, wantRecompute)
	}
}

func TestParallelExecutionMatchesSequentialCommitment(t *testing.T) {
	sequentialConfig := worker.DefaultConfig()
	sequentialConfig.ExecutionWorkers = 1
	sequential, err := worker.NewWithConfig("worker-sequential", sequentialConfig)
	if err != nil {
		t.Fatalf("sequential worker = %v", err)
	}
	parallelConfig := worker.DefaultConfig()
	parallelConfig.ExecutionWorkers = 4
	parallel, err := worker.NewWithConfig("worker-parallel", parallelConfig)
	if err != nil {
		t.Fatalf("parallel worker = %v", err)
	}
	spec := job.JobSpec{
		Version: job.ProtocolVersion, Seed: 1, Steps: 4,
		InputSize: 2, HiddenSize: 3, OutputSize: 1, BatchSize: 2,
		LearningRate: job.DefaultLearningRate,
		Dataset:      job.Dataset{Examples: 2, Inputs: []float32{0.5, -0.25, -0.25, 0.5}, Targets: []float32{0.125, -0.25}},
	}
	machine, err := job.NewMachine(spec)
	if err != nil {
		t.Fatalf("NewMachine() error = %v", err)
	}
	encoded, err := job.MarshalSpec(spec)
	if err != nil {
		t.Fatalf("MarshalSpec() error = %v", err)
	}
	payload := protocol.ExecuteRequest{Meta: protocol.Meta{JobID: protocol.EncodeDigest(machine.ID()), AttemptID: 9}, Job: encoded}
	var sequentialCommitment protocol.ExecuteResponse
	var parallelCommitment protocol.ExecuteResponse
	postJSONDecode(t, sequential, protocol.ExecutePath, payload, &sequentialCommitment)
	postJSONDecode(t, parallel, protocol.ExecutePath, payload, &parallelCommitment)
	if sequentialCommitment.Root != parallelCommitment.Root || sequentialCommitment.FinalStateHash != parallelCommitment.FinalStateHash {
		t.Fatalf("parallel commitment = %+v, sequential = %+v", parallelCommitment, sequentialCommitment)
	}
}

func TestWorkerEnforcesBatchWorkAndRetentionAdmission(t *testing.T) {
	tests := []struct {
		name   string
		limits func(protocol.Limits) protocol.Limits
	}{
		{name: "batch", limits: func(limits protocol.Limits) protocol.Limits { limits.MaxBatchSize = 1; return limits }},
		{name: "work", limits: func(limits protocol.Limits) protocol.Limits { limits.MaxWork = 1; return limits }},
		{name: "retained states", limits: func(limits protocol.Limits) protocol.Limits { limits.MaxRetainedStateBytes = 1; return limits }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limits := tc.limits(protocol.DefaultLimits())
			server, err := worker.New("worker-a", limits)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			spec := job.JobSpec{
				Version: job.ProtocolVersion, Seed: 1, Steps: 1,
				InputSize: 1, HiddenSize: 1, OutputSize: 1,
				BatchSize: 2, LearningRate: job.DefaultLearningRate,
				Dataset: job.Dataset{Examples: 2, Inputs: []float32{0.5, -0.5}, Targets: []float32{0.25, -0.25}},
			}
			machine, err := job.NewMachine(spec)
			if err != nil {
				t.Fatalf("NewMachine() error = %v", err)
			}
			encoded, err := job.MarshalSpec(spec)
			if err != nil {
				t.Fatalf("MarshalSpec() error = %v", err)
			}
			request := protocol.ExecuteRequest{Meta: protocol.Meta{
				JobID: protocol.EncodeDigest(machine.ID()), AttemptID: 1, Round: 0,
			}, Job: encoded}
			if status := postJSON(t, server, protocol.ExecutePath, request); status != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want %d", status, http.StatusUnprocessableEntity)
			}
		})
	}
}

func postJSON(t *testing.T, handler http.Handler, path string, message any) int {
	t.Helper()
	payload, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	request.Header.Set("Content-Type", protocol.JSONContentType)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response.Code
}

func postJSONDecode(t *testing.T, handler http.Handler, path string, message, destination any) {
	t.Helper()
	payload, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	request.Header.Set("Content-Type", protocol.JSONContentType)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("%s status = %d, body = %s", path, response.Code, response.Body.String())
	}
	if err := json.Unmarshal(response.Body.Bytes(), destination); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", path, err)
	}
}
