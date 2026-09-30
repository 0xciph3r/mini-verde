package coordinator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/0xciph3r/mini-verde/internal/coordinator"
	"github.com/0xciph3r/mini-verde/internal/job"
	"github.com/0xciph3r/mini-verde/internal/protocol"
	"github.com/0xciph3r/mini-verde/internal/worker"
)

func TestTwoHonestWorkersCompleteOneHundredJobs(t *testing.T) {
	limits := protocol.DefaultLimits()
	workerA, serverA := startWorker(t, "worker-a", limits, nil)
	workerB, serverB := startWorker(t, "worker-b", limits, nil)
	client := mustCoordinator(t, serverA.URL, serverB.URL, limits)

	for run := range 100 {
		spec := smallSpec(uint64(run + 1))
		result, err := client.Run(context.Background(), spec)
		if err != nil {
			t.Fatalf("Run(%d) error = %v", run, err)
		}
		if result.Status != coordinator.Accepted {
			t.Fatalf("Run(%d) status = %q, want %q", run, result.Status, coordinator.Accepted)
		}
		if len(result.Commitments) != 2 {
			t.Fatalf("Run(%d) commitments = %d, want 2", run, len(result.Commitments))
		}
	}
	if got := workerA.ActiveAttempts(); got != 0 {
		t.Fatalf("worker-a active attempts = %d, want 0", got)
	}
	if got := workerB.ActiveAttempts(); got != 0 {
		t.Fatalf("worker-b active attempts = %d, want 0", got)
	}
	if got := workerA.ClosedAttempts(); got != 100 {
		t.Fatalf("worker-a closed attempts = %d, want 100", got)
	}
	if got := workerB.ClosedAttempts(); got != 100 {
		t.Fatalf("worker-b closed attempts = %d, want 100", got)
	}
}

func TestCoordinatorExecutesWorkersConcurrently(t *testing.T) {
	limits := protocol.DefaultLimits()
	gate := newOverlapGate()
	_, serverA := startWorker(t, "worker-a", limits, gate.Wrap)
	_, serverB := startWorker(t, "worker-b", limits, gate.Wrap)
	client := mustCoordinator(t, serverA.URL, serverB.URL, limits)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := client.Run(ctx, smallSpec(1))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Accepted {
		t.Fatalf("Run() status = %q, want %q", result.Status, coordinator.Accepted)
	}
	if gate.Arrivals() != 2 {
		t.Fatalf("concurrent execute arrivals = %d, want 2", gate.Arrivals())
	}
}

func TestDifferentRootsWithSameFinalStateAreAccepted(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, serverA := startWorker(t, "worker-a", limits, mutateExecuteRoot)
	_, serverB := startWorker(t, "worker-b", limits, nil)
	client := mustCoordinator(t, serverA.URL, serverB.URL, limits)
	result, err := client.Run(context.Background(), smallSpec(1))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Accepted {
		t.Fatalf("Run() status = %q, want %q", result.Status, coordinator.Accepted)
	}
	if result.Commitments[0].Root == result.Commitments[1].Root {
		t.Fatal("test did not produce different roots")
	}
}

func TestDifferentFinalHashesNeedDispute(t *testing.T) {
	limits := protocol.DefaultLimits()
	workerA, serverA := startWorker(t, "worker-a", limits, mutateExecuteFinalHash)
	workerB, serverB := startWorker(t, "worker-b", limits, nil)
	client := mustCoordinator(t, serverA.URL, serverB.URL, limits)
	result, err := client.Run(context.Background(), smallSpec(1))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.NeedsDispute {
		t.Fatalf("Run() status = %q, want %q", result.Status, coordinator.NeedsDispute)
	}
	if workerA.ActiveAttempts() != 1 || workerB.ActiveAttempts() != 1 {
		t.Fatal("disputed attempts were closed")
	}
}

func TestBadPreferredStateFallsBackToSecondWorker(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, serverA := startWorker(t, "worker-a", limits, mutateFinalState)
	_, serverB := startWorker(t, "worker-b", limits, nil)
	client := mustCoordinator(t, serverA.URL, serverB.URL, limits)
	result, err := client.Run(context.Background(), smallSpec(1))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Accepted {
		t.Fatalf("Run() status = %q, want %q", result.Status, coordinator.Accepted)
	}
	if len(result.Findings) != 1 || result.Findings[0].WorkerID != "worker-a" || result.Findings[0].Verdict != "BadState" {
		t.Fatalf("Run() findings = %+v, want worker-a BadState", result.Findings)
	}
}

func TestMismatchedResponseMetadataIsRejected(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, serverA := startWorker(t, "worker-a", limits, mutateExecuteRound)
	_, serverB := startWorker(t, "worker-b", limits, nil)
	client := mustCoordinator(t, serverA.URL, serverB.URL, limits)
	if _, err := client.Run(context.Background(), smallSpec(1)); err == nil {
		t.Fatal("Run() accepted mismatched response metadata")
	}
}

func TestCoordinatorEnforcesAdmissionLimitsBeforeDispatch(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, serverA := startWorker(t, "worker-a", limits, nil)
	_, serverB := startWorker(t, "worker-b", limits, nil)
	client := mustCoordinator(t, serverA.URL, serverB.URL, limits)
	spec := smallSpec(1)
	spec.Steps = limits.MaxSteps + 1
	if _, err := client.Run(context.Background(), spec); err == nil {
		t.Fatal("Run() accepted a job above the step limit")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, serverA := startWorker(t, "worker-a", limits, nil)
	_, serverB := startWorker(t, "worker-b", limits, nil)
	client := mustCoordinator(t, serverA.URL, serverB.URL, limits)
	result, err := client.Run(context.Background(), smallSpec(1))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	commitment := result.Commitments[0]
	request := protocol.CloseRequest{Meta: protocol.Meta{
		JobID: protocol.EncodeDigest(result.JobID), AttemptID: commitment.AttemptID, Round: 2,
	}}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	response, err := http.Post(serverA.URL+protocol.ClosePath, protocol.JSONContentType, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("second close request error = %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("second close status = %d, body = %s", response.StatusCode, payload)
	}
}

func smallSpec(seed uint64) job.JobSpec {
	return job.JobSpec{
		Version: job.ProtocolVersion, Seed: seed, Steps: 4,
		InputSize: 2, HiddenSize: 4, OutputSize: 1,
		BatchSize: 2, LearningRate: job.DefaultLearningRate,
		Dataset: job.Dataset{
			Examples: 4,
			Inputs:   []float32{0.5, -0.25, -0.5, 0.75, 0.25, 0.125, -0.75, -0.5},
			Targets:  []float32{0.25, -0.5, 0.125, 0.75},
		},
	}
}

func startWorker(t *testing.T, id string, limits protocol.Limits, wrap func(http.Handler) http.Handler) (*worker.Server, *httptest.Server) {
	t.Helper()
	workerServer, err := worker.New(id, limits)
	if err != nil {
		t.Fatalf("worker.New() error = %v", err)
	}
	var handler http.Handler = workerServer
	if wrap != nil {
		handler = wrap(handler)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return workerServer, server
}

func mustCoordinator(t *testing.T, workerAURL, workerBURL string, limits protocol.Limits) *coordinator.Coordinator {
	t.Helper()
	client, err := coordinator.New([]coordinator.Endpoint{
		{ID: "worker-a", URL: workerAURL},
		{ID: "worker-b", URL: workerBURL},
	}, &http.Client{}, limits)
	if err != nil {
		t.Fatalf("coordinator.New() error = %v", err)
	}
	return client
}

type overlapGate struct {
	mu       sync.Mutex
	arrivals int
	release  chan struct{}
}

func newOverlapGate() *overlapGate {
	return &overlapGate{release: make(chan struct{})}
}

func (g *overlapGate) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == protocol.ExecutePath {
			g.mu.Lock()
			g.arrivals++
			if g.arrivals == 2 {
				close(g.release)
			}
			g.mu.Unlock()
			select {
			case <-g.release:
			case <-r.Context().Done():
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (g *overlapGate) Arrivals() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.arrivals
}

func mutateExecuteRoot(next http.Handler) http.Handler {
	return mutateResponse(next, protocol.ExecutePath, func(response any) {
		message := response.(*protocol.ExecuteResponse)
		message.Root = flipHex(message.Root)
	})
}

func mutateExecuteFinalHash(next http.Handler) http.Handler {
	return mutateResponse(next, protocol.ExecutePath, func(response any) {
		message := response.(*protocol.ExecuteResponse)
		message.FinalStateHash = flipHex(message.FinalStateHash)
	})
}

func mutateExecuteRound(next http.Handler) http.Handler {
	return mutateResponse(next, protocol.ExecutePath, func(response any) {
		message := response.(*protocol.ExecuteResponse)
		message.Round++
	})
}

func mutateFinalState(next http.Handler) http.Handler {
	return mutateResponse(next, protocol.FinalStatePath, func(response any) {
		message := response.(*protocol.FinalStateResponse)
		message.State[0] ^= 1
	})
}

func mutateResponse(next http.Handler, path string, mutate func(any)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path {
			next.ServeHTTP(w, r)
			return
		}
		recorder := httptest.NewRecorder()
		next.ServeHTTP(recorder, r)
		if recorder.Code != http.StatusOK {
			copyResponse(w, recorder)
			return
		}
		var message any
		if path == protocol.ExecutePath {
			message = &protocol.ExecuteResponse{}
		} else {
			message = &protocol.FinalStateResponse{}
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), message); err != nil {
			panic(err)
		}
		mutate(message)
		payload, err := json.Marshal(message)
		if err != nil {
			panic(err)
		}
		w.Header().Set("Content-Type", protocol.JSONContentType)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	})
}

func copyResponse(w http.ResponseWriter, recorder *httptest.ResponseRecorder) {
	for key, values := range recorder.Header() {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(recorder.Code)
	_, _ = io.Copy(w, recorder.Body)
}

func flipHex(value string) string {
	if strings.HasPrefix(value, "0") {
		return "1" + value[1:]
	}
	return "0" + value[1:]
}
