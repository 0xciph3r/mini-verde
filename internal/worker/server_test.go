package worker_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/0xciph3r/mini-verde/internal/job"
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
