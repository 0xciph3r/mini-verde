package coordinator_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/0xciph3r/mini-verde/internal/coordinator"
	"github.com/0xciph3r/mini-verde/internal/job"
	"github.com/0xciph3r/mini-verde/internal/protocol"
	"github.com/0xciph3r/mini-verde/internal/worker"
)

func TestM5DeterministicComputationalBehaviors(t *testing.T) {
	tests := []struct {
		name        string
		behavior    worker.Behavior
		wantVerdict string
		wantStep    func(*job.Machine) uint64
	}{
		{
			name: "corrupt late", behavior: worker.BehaviorCorruptLate,
			wantVerdict: protocol.VerdictWrongStep,
			wantStep: func(machine *job.Machine) uint64 {
				return worker.FaultStep(machine, worker.BehaviorCorruptLate, 91)
			},
		},
		{
			name: "corrupt all", behavior: worker.BehaviorCorruptAll,
			wantVerdict: protocol.VerdictWrongStep,
			wantStep:    func(*job.Machine) uint64 { return 1 },
		},
		{
			name: "lazy", behavior: worker.BehaviorLazy,
			wantVerdict: protocol.VerdictWrongStep,
			wantStep:    func(machine *job.Machine) uint64 { return (machine.LeafCount()-1)/2 + 1 },
		},
		{
			name: "liar on bisect", behavior: worker.BehaviorLiarOnBisect,
			wantVerdict: protocol.VerdictBadOpening,
			wantStep:    nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limits := protocol.DefaultLimits()
			_, honestServer := startWorker(t, "honest", limits, nil)
			_, faultyServer := startBehaviorWorker(t, "faulty", limits, tc.behavior, 91, 0)
			client := mustCoordinatorPool(t, coordinator.DefaultConfig(),
				coordinator.Endpoint{ID: "honest", URL: honestServer.URL},
				coordinator.Endpoint{ID: "faulty", URL: faultyServer.URL},
			)
			spec := smallSpec(17)
			spec.Steps = 16
			machine, err := job.NewMachine(spec)
			if err != nil {
				t.Fatalf("NewMachine() error = %v", err)
			}
			result, err := client.Run(context.Background(), spec)
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if result.Status != coordinator.Accepted || len(result.Findings) != 1 {
				t.Fatalf("result = %+v", result)
			}
			finding := result.Findings[0]
			if finding.WorkerID != "faulty" || finding.Verdict != tc.wantVerdict {
				t.Fatalf("finding = %+v", finding)
			}
			if tc.wantStep != nil {
				want := tc.wantStep(machine)
				if finding.Step == nil || *finding.Step != want {
					t.Fatalf("finding step = %v, want %d", finding.Step, want)
				}
			}
		})
	}
}

func TestExecuteTimeoutReassignsWithoutAcceptingByTimeout(t *testing.T) {
	limits := protocol.DefaultLimits()
	stalledWorker, stalled := startBehaviorWorker(t, "stalled", limits, worker.BehaviorStaller, 0, 0)
	workerB, serverB := startWorker(t, "worker-b", limits, nil)
	workerC, serverC := startWorker(t, "worker-c", limits, nil)
	config := coordinator.DefaultConfig()
	config.Limits = limits
	config.RequestTimeout = 30 * time.Millisecond
	config.JobTimeout = time.Second
	client := mustCoordinatorPool(t, config,
		coordinator.Endpoint{ID: "stalled", URL: stalled.URL},
		coordinator.Endpoint{ID: "worker-b", URL: serverB.URL},
		coordinator.Endpoint{ID: "worker-c", URL: serverC.URL},
	)

	result, err := client.Run(context.Background(), smallSpec(1))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Accepted || result.Replacements != 1 || result.PairGenerations != 1 {
		t.Fatalf("result status/reassignment = %+v", result)
	}
	if len(result.Findings) != 1 || result.Findings[0].WorkerID != "stalled" ||
		result.Findings[0].Verdict != protocol.VerdictTimeout {
		t.Fatalf("findings = %+v", result.Findings)
	}
	if result.Acceptance == nil || result.Acceptance.Kind != coordinator.BasisFinalStateAgreement {
		t.Fatalf("acceptance = %+v", result.Acceptance)
	}
	if workerB.ActiveAttempts() != 0 || workerC.ActiveAttempts() != 0 {
		t.Fatal("accepted replacement pair was not closed")
	}
	if stalledWorker.ActiveAttempts() != 1 {
		t.Fatal("unreachable staller did not retain its fenced attempt")
	}
	if got := result.Reputation["stalled"].Timeouts; got != 1 {
		t.Fatalf("stalled timeout reputation = %d, want 1", got)
	}
	if result.Reputation["stalled"].Losses != 0 || result.Reputation["worker-b"].Wins != 1 ||
		result.Reputation["worker-c"].Wins != 1 {
		t.Fatalf("reputation = %+v", result.Reputation)
	}
	events := client.ReputationEvents()
	if len(events) != 3 || events[0].Sequence != 1 || events[0].WorkerID != "stalled" ||
		events[0].Kind != "timeout" || events[1].WorkerID != "worker-c" || events[2].WorkerID != "worker-b" {
		t.Fatalf("reputation events = %+v", events)
	}
}

func TestCrashAndSlowBehaviorsUseAvailabilityPath(t *testing.T) {
	tests := []struct {
		name     string
		behavior worker.Behavior
		delay    time.Duration
	}{
		{name: "crasher", behavior: worker.BehaviorCrasher},
		{name: "slow beyond deadline", behavior: worker.BehaviorSlow, delay: 100 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limits := protocol.DefaultLimits()
			_, unavailable := startBehaviorWorker(t, "unavailable", limits, tc.behavior, 7, tc.delay)
			_, serverB := startWorker(t, "worker-b", limits, nil)
			_, serverC := startWorker(t, "worker-c", limits, nil)
			config := coordinator.DefaultConfig()
			config.Limits = limits
			config.RequestTimeout = 25 * time.Millisecond
			config.JobTimeout = time.Second
			client := mustCoordinatorPool(t, config,
				coordinator.Endpoint{ID: "unavailable", URL: unavailable.URL},
				coordinator.Endpoint{ID: "worker-b", URL: serverB.URL},
				coordinator.Endpoint{ID: "worker-c", URL: serverC.URL},
			)
			result, err := client.Run(context.Background(), smallSpec(1))
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if result.Status != coordinator.Accepted || len(result.Findings) != 1 ||
				result.Findings[0].Verdict != protocol.VerdictTimeout {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func TestSlowWorkerInsideDeadlineIsAccepted(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, slow := startBehaviorWorker(t, "slow", limits, worker.BehaviorSlow, 0, 5*time.Millisecond)
	_, honest := startWorker(t, "honest", limits, nil)
	config := coordinator.DefaultConfig()
	config.Limits = limits
	config.RequestTimeout = 100 * time.Millisecond
	config.JobTimeout = time.Second
	client := mustCoordinatorPool(t, config,
		coordinator.Endpoint{ID: "slow", URL: slow.URL},
		coordinator.Endpoint{ID: "honest", URL: honest.URL},
	)
	result, err := client.Run(context.Background(), smallSpec(1))
	if err != nil || result.Status != coordinator.Accepted || len(result.Findings) != 0 {
		t.Fatalf("result = %+v, error = %v", result, err)
	}
}

func TestFinalStateTimeoutFallsBackBeforeReassignment(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, serverA := startWorker(t, "worker-a", limits, stallResponse(protocol.FinalStatePath))
	_, serverB := startWorker(t, "worker-b", limits, nil)
	config := coordinator.DefaultConfig()
	config.Limits = limits
	config.RequestTimeout = 25 * time.Millisecond
	config.JobTimeout = time.Second
	client := mustCoordinatorPool(t, config,
		coordinator.Endpoint{ID: "worker-a", URL: serverA.URL},
		coordinator.Endpoint{ID: "worker-b", URL: serverB.URL},
	)

	result, err := client.Run(context.Background(), smallSpec(1))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Accepted || result.Replacements != 0 || len(result.Findings) != 1 {
		t.Fatalf("result = %+v", result)
	}
	if result.Findings[0].WorkerID != "worker-a" || result.Findings[0].Verdict != protocol.VerdictTimeout {
		t.Fatalf("finding = %+v", result.Findings[0])
	}
}

func TestStepHashTimeoutReplacesWorkerAndRestartsPair(t *testing.T) {
	limits := protocol.DefaultLimits()
	wrap := func(next http.Handler) http.Handler {
		return stallResponse(protocol.StepHashPath)(mutateExecuteFinalHash(next))
	}
	_, serverA := startWorker(t, "worker-a", limits, wrap)
	_, serverB := startWorker(t, "worker-b", limits, nil)
	_, serverC := startWorker(t, "worker-c", limits, nil)
	config := coordinator.DefaultConfig()
	config.Limits = limits
	config.RequestTimeout = 25 * time.Millisecond
	config.JobTimeout = time.Second
	client := mustCoordinatorPool(t, config,
		coordinator.Endpoint{ID: "worker-a", URL: serverA.URL},
		coordinator.Endpoint{ID: "worker-b", URL: serverB.URL},
		coordinator.Endpoint{ID: "worker-c", URL: serverC.URL},
	)

	result, err := client.Run(context.Background(), smallSpec(1))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Accepted || result.Replacements != 1 || result.PairGenerations != 2 {
		t.Fatalf("result = %+v", result)
	}
	if len(result.PairDisputes) != 1 || result.PairDisputes[0].PairGeneration != 1 {
		t.Fatalf("pair disputes = %+v", result.PairDisputes)
	}
	if len(result.Findings) != 1 || result.Findings[0].Verdict != protocol.VerdictTimeout ||
		result.Findings[0].Phase != "step-hash" {
		t.Fatalf("findings = %+v", result.Findings)
	}
}

func TestProvenWinnerTimeoutUsesReplacementWithoutTrustingFaultyPeer(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, faulty := startBehaviorWorker(t, "faulty", limits, worker.BehaviorCorruptAll, 0, 0)
	_, winner := startWorker(t, "winner", limits, stallResponse(protocol.FinalStatePath))
	_, replacement := startWorker(t, "replacement", limits, nil)
	config := coordinator.DefaultConfig()
	config.Limits = limits
	config.RequestTimeout = 25 * time.Millisecond
	config.JobTimeout = time.Second
	client := mustCoordinatorPool(t, config,
		coordinator.Endpoint{ID: "faulty", URL: faulty.URL},
		coordinator.Endpoint{ID: "winner", URL: winner.URL},
		coordinator.Endpoint{ID: "replacement", URL: replacement.URL},
	)

	result, err := client.Run(context.Background(), smallSpec(1))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Accepted || result.Replacements != 1 || result.PairGenerations != 2 {
		t.Fatalf("result = %+v", result)
	}
	if result.Acceptance == nil || result.Acceptance.Kind != coordinator.BasisLoserProvenFaulty ||
		result.Acceptance.FaultyWorker != "faulty" || result.Dispute == nil ||
		result.Dispute.WinnerID != "replacement" {
		t.Fatalf("acceptance/dispute = %+v / %+v", result.Acceptance, result.Dispute)
	}
	if len(result.PairDisputes) != 2 || result.PairDisputes[0].PairGeneration != 1 ||
		result.PairDisputes[1].PairGeneration != 2 {
		t.Fatalf("pair disputes = %+v", result.PairDisputes)
	}
	if len(result.Findings) != 2 || result.Findings[0].WorkerID != "winner" ||
		result.Findings[0].Verdict != protocol.VerdictTimeout ||
		result.Findings[1].WorkerID != "faulty" || result.Findings[1].Verdict != protocol.VerdictWrongStep {
		t.Fatalf("findings = %+v", result.Findings)
	}
}

func TestPoolExhaustionIsUnresolvedWithoutComputationalBlame(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, stalledA := startBehaviorWorker(t, "stalled-a", limits, worker.BehaviorStaller, 0, 0)
	_, stalledB := startBehaviorWorker(t, "stalled-b", limits, worker.BehaviorStaller, 0, 0)
	config := coordinator.DefaultConfig()
	config.Limits = limits
	config.RequestTimeout = 20 * time.Millisecond
	config.JobTimeout = time.Second
	client := mustCoordinatorPool(t, config,
		coordinator.Endpoint{ID: "stalled-a", URL: stalledA.URL},
		coordinator.Endpoint{ID: "stalled-b", URL: stalledB.URL},
	)

	result, err := client.Run(context.Background(), smallSpec(1))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Unresolved || len(result.Findings) != 2 {
		t.Fatalf("result = %+v", result)
	}
	for _, finding := range result.Findings {
		if finding.Verdict != protocol.VerdictTimeout || finding.Step != nil {
			t.Fatalf("finding = %+v", finding)
		}
	}
}

func TestUnresolvedResultPreservesObjectiveEvidenceAlreadyCollected(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, serverA := startWorker(t, "worker-a", limits, stallResponse(protocol.FinalStatePath))
	_, serverB := startWorker(t, "worker-b", limits, mutateFinalState)
	config := coordinator.DefaultConfig()
	config.Limits = limits
	config.RequestTimeout = 20 * time.Millisecond
	config.JobTimeout = time.Second
	client := mustCoordinatorPool(t, config,
		coordinator.Endpoint{ID: "worker-a", URL: serverA.URL},
		coordinator.Endpoint{ID: "worker-b", URL: serverB.URL},
	)

	result, err := client.Run(context.Background(), smallSpec(1))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Unresolved || len(result.Findings) != 2 {
		t.Fatalf("result = %+v", result)
	}
	if result.Findings[0].Verdict != protocol.VerdictTimeout ||
		result.Findings[1].Verdict != protocol.VerdictBadState {
		t.Fatalf("findings = %+v", result.Findings)
	}
	if result.Reputation["worker-a"].Timeouts != 1 || result.Reputation["worker-b"].Losses != 1 {
		t.Fatalf("reputation = %+v", result.Reputation)
	}
}

func TestTwoUnavailableSlotsAreReplacedTogether(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, stalledA := startBehaviorWorker(t, "stalled-a", limits, worker.BehaviorStaller, 0, 0)
	_, stalledB := startBehaviorWorker(t, "stalled-b", limits, worker.BehaviorStaller, 0, 0)
	_, honestC := startWorker(t, "honest-c", limits, nil)
	_, honestD := startWorker(t, "honest-d", limits, nil)
	config := coordinator.DefaultConfig()
	config.Limits = limits
	config.RequestTimeout = 20 * time.Millisecond
	config.JobTimeout = time.Second
	client := mustCoordinatorPool(t, config,
		coordinator.Endpoint{ID: "stalled-a", URL: stalledA.URL},
		coordinator.Endpoint{ID: "stalled-b", URL: stalledB.URL},
		coordinator.Endpoint{ID: "honest-c", URL: honestC.URL},
		coordinator.Endpoint{ID: "honest-d", URL: honestD.URL},
	)

	result, err := client.Run(context.Background(), smallSpec(1))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Accepted || result.Replacements != 2 || len(result.Findings) != 2 {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Commitments) != 2 || result.Commitments[0].WorkerID != "honest-c" ||
		result.Commitments[1].WorkerID != "honest-d" {
		t.Fatalf("final commitments = %+v", result.Commitments)
	}
}

func TestCallerCancellationIsNotAWorkerTimeout(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, stalledA := startBehaviorWorker(t, "stalled-a", limits, worker.BehaviorStaller, 0, 0)
	_, stalledB := startBehaviorWorker(t, "stalled-b", limits, worker.BehaviorStaller, 0, 0)
	config := coordinator.DefaultConfig()
	config.Limits = limits
	config.RequestTimeout = time.Second
	config.JobTimeout = 2 * time.Second
	client := mustCoordinatorPool(t, config,
		coordinator.Endpoint{ID: "stalled-a", URL: stalledA.URL},
		coordinator.Endpoint{ID: "stalled-b", URL: stalledB.URL},
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := client.Run(ctx, smallSpec(1))
	if err == nil || len(result.Findings) != 0 {
		t.Fatalf("result = %+v, error = %v", result, err)
	}
}

func TestLateExecuteResponseCannotAffectReplacementGeneration(t *testing.T) {
	limits := protocol.DefaultLimits()
	delayExecute := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			if request.URL.Path != protocol.ExecutePath {
				next.ServeHTTP(response, request)
				return
			}
			recorder := httptest.NewRecorder()
			next.ServeHTTP(recorder, request)
			time.Sleep(60 * time.Millisecond)
			copyResponse(response, recorder)
		})
	}
	workerA, serverA := startWorker(t, "worker-a", limits, delayExecute)
	_, serverB := startWorker(t, "worker-b", limits, nil)
	_, serverC := startWorker(t, "worker-c", limits, nil)
	config := coordinator.DefaultConfig()
	config.Limits = limits
	config.RequestTimeout = 15 * time.Millisecond
	config.JobTimeout = time.Second
	client := mustCoordinatorPool(t, config,
		coordinator.Endpoint{ID: "worker-a", URL: serverA.URL},
		coordinator.Endpoint{ID: "worker-b", URL: serverB.URL},
		coordinator.Endpoint{ID: "worker-c", URL: serverC.URL},
	)

	result, err := client.Run(context.Background(), smallSpec(1))
	if err != nil || result.Status != coordinator.Accepted || result.Replacements != 1 {
		t.Fatalf("result = %+v, error = %v", result, err)
	}
	time.Sleep(70 * time.Millisecond)
	if workerA.ActiveAttempts() != 0 {
		t.Fatal("late execute response revived a closed attempt")
	}
}

func startBehaviorWorker(
	t *testing.T,
	id string,
	limits protocol.Limits,
	behavior worker.Behavior,
	seed uint64,
	delay time.Duration,
) (*worker.Server, *httptest.Server) {
	t.Helper()
	config := worker.DefaultConfig()
	config.Limits = limits
	config.Behavior = behavior
	config.FaultSeed = seed
	config.Delay = delay
	workerServer, err := worker.NewWithConfig(id, config)
	if err != nil {
		t.Fatalf("worker.NewWithConfig() error = %v", err)
	}
	server := httptest.NewServer(workerServer)
	t.Cleanup(func() {
		server.CloseClientConnections()
		server.Close()
	})
	return workerServer, server
}

func mustCoordinatorPool(
	t *testing.T,
	config coordinator.Config,
	endpoints ...coordinator.Endpoint,
) *coordinator.Coordinator {
	t.Helper()
	client, err := coordinator.NewWithConfig(endpoints, &http.Client{}, config)
	if err != nil {
		t.Fatalf("coordinator.NewWithConfig() error = %v", err)
	}
	return client
}

func waitForCoordinator(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not met before deadline")
		}
		time.Sleep(time.Millisecond)
	}
}
