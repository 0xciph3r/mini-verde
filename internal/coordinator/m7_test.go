package coordinator_test

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/0xciph3r/mini-verde/internal/coordinator"
	"github.com/0xciph3r/mini-verde/internal/protocol"
	"github.com/0xciph3r/mini-verde/internal/worker"
)

func TestStrictVerifierRejectsEqualWrongResultsFromColludingWorkers(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, serverA := startBehaviorWorker(t, "worker-a", limits, worker.BehaviorCorruptAll, 0, 0)
	_, serverB := startBehaviorWorker(t, "worker-b", limits, worker.BehaviorCorruptAll, 0, 0)
	client := mustCoordinatorPool(t, coordinator.DefaultConfig(),
		coordinator.Endpoint{ID: "worker-a", URL: serverA.URL},
		coordinator.Endpoint{ID: "worker-b", URL: serverB.URL},
	)
	result, err := client.Run(context.Background(), smallSpec(19))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Rejected || len(result.Findings) != 2 {
		t.Fatalf("result = %+v, want strict rejection with two BadState findings", result)
	}
	for _, finding := range result.Findings {
		if finding.Verdict != protocol.VerdictBadState {
			t.Fatalf("finding = %+v, want BadState", finding)
		}
	}
}

func TestCoordinatorWALPersistsTerminalResultAndRecoverDoesNotReexecute(t *testing.T) {
	limits := protocol.DefaultLimits()
	workerA, serverA := startWorker(t, "worker-a", limits, nil)
	workerB, serverB := startWorker(t, "worker-b", limits, nil)
	path := filepath.Join(t.TempDir(), "coordinator.jsonl")
	config := coordinator.DefaultConfig()
	config.WALPath = path
	client := mustCoordinatorPool(t, config,
		coordinator.Endpoint{ID: "worker-a", URL: serverA.URL},
		coordinator.Endpoint{ID: "worker-b", URL: serverB.URL},
	)
	spec := smallSpec(23)
	first, err := client.Run(context.Background(), spec)
	if err != nil || first.Status != coordinator.Accepted {
		t.Fatalf("first run = %+v, error = %v", first, err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close first coordinator = %v", err)
	}
	firstExecutions := workerA.ExecutionCount() + workerB.ExecutionCount()

	restarted, err := coordinator.NewWithConfig([]coordinator.Endpoint{
		{ID: "worker-a", URL: serverA.URL}, {ID: "worker-b", URL: serverB.URL},
	}, &http.Client{}, coordinator.Config{
		Limits:         limits,
		RequestTimeout: coordinator.DefaultRequestTimeout,
		JobTimeout:     coordinator.DefaultJobTimeout,
		WALPath:        path,
	})
	if err != nil {
		t.Fatalf("restart coordinator = %v", err)
	}
	defer restarted.Close()
	recovered, err := restarted.Recover(context.Background(), protocol.EncodeDigest(first.JobID))
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if recovered.Status != coordinator.Accepted || recovered.StateHash != first.StateHash {
		t.Fatalf("recovered result = %+v, want original accepted result", recovered)
	}
	if got := workerA.ExecutionCount() + workerB.ExecutionCount(); got != firstExecutions {
		t.Fatalf("recovery reexecuted workers: executions = %d, want %d", got, firstExecutions)
	}
}
