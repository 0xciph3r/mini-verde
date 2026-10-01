package coordinator_test

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0xciph3r/mini-verde/internal/coordinator"
	"github.com/0xciph3r/mini-verde/internal/dispute"
	"github.com/0xciph3r/mini-verde/internal/job"
	"github.com/0xciph3r/mini-verde/internal/merkle"
	"github.com/0xciph3r/mini-verde/internal/protocol"
)

func TestCorruptLateWorkerIsBlamedAtExactStepForFiveHundredSeeds(t *testing.T) {
	limits := protocol.DefaultLimits()
	honest, honestServer := startWorker(t, "worker-a", limits, nil)
	corrupt := newTraceWorker("worker-b", func(spec job.JobSpec) uint64 { return 1 + spec.Seed%spec.Steps }, 0)
	corruptServer := httptest.NewServer(corrupt)
	t.Cleanup(corruptServer.Close)
	client := mustCoordinator(t, honestServer.URL, corruptServer.URL, limits)

	var expectedRecompute uint64
	for seed := uint64(1); seed <= 500; seed++ {
		spec := smallSpec(seed)
		spec.Steps = 32
		corruptAt := uint64(1) + seed%spec.Steps
		result, err := client.Run(context.Background(), spec)
		if err != nil {
			t.Fatalf("seed %d: Run() error = %v", seed, err)
		}
		if result.Status != coordinator.Accepted {
			t.Fatalf("seed %d: status = %q", seed, result.Status)
		}
		if len(result.Findings) != 1 || result.Findings[0].WorkerID != "worker-b" ||
			result.Findings[0].Verdict != protocol.VerdictWrongStep || result.Findings[0].Step == nil ||
			*result.Findings[0].Step != corruptAt {
			t.Fatalf("seed %d: findings = %+v, want worker-b WrongStep at %d", seed, result.Findings, corruptAt)
		}
		if result.Acceptance == nil || result.Acceptance.Kind != coordinator.BasisLoserProvenFaulty {
			t.Fatalf("seed %d: acceptance = %+v", seed, result.Acceptance)
		}
		if result.Dispute == nil || result.Dispute.HighStep != corruptAt || result.Dispute.Cost.RefereeSteps != 1 {
			t.Fatalf("seed %d: dispute = %+v", seed, result.Dispute)
		}
		if result.Dispute.Cost.BisectionRounds > dispute.CeilLog2(spec.Steps) {
			t.Fatalf("seed %d: bisection rounds = %d", seed, result.Dispute.Cost.BisectionRounds)
		}
		if result.Dispute.Cost.LogicalRounds > dispute.CeilLog2(spec.Steps)+5 {
			t.Fatalf("seed %d: logical rounds = %d", seed, result.Dispute.Cost.LogicalRounds)
		}
		if first, ok := corrupt.firstOpening(result.Commitments[1].AttemptID); !ok || first != spec.Steps {
			t.Fatalf("seed %d: first opening = %d, %v; want final step %d", seed, first, ok, spec.Steps)
		}
		expectedRecompute += (corruptAt - 1) % protocol.CheckpointInterval
	}
	if got := honest.RecomputeCount(); got != expectedRecompute {
		t.Fatalf("honest worker recompute count = %d, schedule expects %d", got, expectedRecompute)
	}
}

func TestValidNonCanonicalLeafZeroIsWrongStepZero(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, honestServer := startWorker(t, "worker-a", limits, nil)
	wrongInitial := newTraceWorker("worker-b", func(job.JobSpec) uint64 { return 0 }, 0)
	wrongServer := httptest.NewServer(wrongInitial)
	t.Cleanup(wrongServer.Close)
	client := mustCoordinator(t, honestServer.URL, wrongServer.URL, limits)

	result, err := client.Run(context.Background(), smallSpec(1))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Accepted || len(result.Findings) != 1 {
		t.Fatalf("result = %+v", result)
	}
	finding := result.Findings[0]
	if finding.Verdict != protocol.VerdictWrongStep || finding.Step == nil || *finding.Step != 0 {
		t.Fatalf("finding = %+v, want WrongStep at zero", finding)
	}
}

func TestInvalidMerkleProofIsBadOpening(t *testing.T) {
	limits := protocol.DefaultLimits()
	wrap := func(next http.Handler) http.Handler {
		return mutateExecuteFinalHash(mutateStepHashProof(next))
	}
	_, serverA := startWorker(t, "worker-a", limits, wrap)
	_, serverB := startWorker(t, "worker-b", limits, nil)
	client := mustCoordinator(t, serverA.URL, serverB.URL, limits)
	// The execute hash mutation creates disagreement; the invalid proof then
	// independently proves worker-a faulty.
	result, err := client.Run(context.Background(), smallSpec(1))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Accepted || len(result.Findings) != 1 ||
		result.Findings[0].Verdict != protocol.VerdictBadOpening {
		t.Fatalf("result = %+v", result)
	}
}

func TestLiarOnFirstMidpointGetsBadOpening(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, honestServer := startWorker(t, "worker-a", limits, nil)
	corrupt := newTraceWorker("worker-b", func(job.JobSpec) uint64 { return 2 }, 0)
	corruptServer := httptest.NewServer(mutateNthStepHashProof(corrupt, 3))
	t.Cleanup(corruptServer.Close)
	client := mustCoordinator(t, honestServer.URL, corruptServer.URL, limits)

	result, err := client.Run(context.Background(), smallSpec(7))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Accepted || len(result.Findings) != 1 ||
		result.Findings[0].WorkerID != "worker-b" || result.Findings[0].Verdict != protocol.VerdictBadOpening {
		t.Fatalf("result = %+v", result)
	}
}

func TestBothWrongIsDerivedFromTwoIndependentWrongSteps(t *testing.T) {
	limits := protocol.DefaultLimits()
	workerA := newTraceWorker("worker-a", func(job.JobSpec) uint64 { return 1 }, 0)
	workerB := newTraceWorker("worker-b", func(job.JobSpec) uint64 { return 1 }, 1)
	serverA := httptest.NewServer(workerA)
	serverB := httptest.NewServer(workerB)
	t.Cleanup(serverA.Close)
	t.Cleanup(serverB.Close)
	client := mustCoordinator(t, serverA.URL, serverB.URL, limits)

	result, err := client.Run(context.Background(), smallSpec(9))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Rejected || result.OverallVerdict != protocol.VerdictBothWrong {
		t.Fatalf("result status/verdict = %q/%q", result.Status, result.OverallVerdict)
	}
	if len(result.Findings) != 2 {
		t.Fatalf("findings = %+v", result.Findings)
	}
	for _, finding := range result.Findings {
		if finding.Verdict != protocol.VerdictWrongStep || finding.Step == nil || *finding.Step != 1 {
			t.Fatalf("finding = %+v", finding)
		}
	}
}

func TestBothBadAgreedStatesRejectWithoutReferee(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, honestServer := startWorker(t, "worker-a", limits, mutateIndexedState)
	corrupt := newTraceWorker("worker-b", func(job.JobSpec) uint64 { return 2 }, 0)
	corrupt.badIndexedState = true
	corruptServer := httptest.NewServer(corrupt)
	t.Cleanup(corruptServer.Close)
	client := mustCoordinator(t, honestServer.URL, corruptServer.URL, limits)

	result, err := client.Run(context.Background(), smallSpec(3))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Rejected || len(result.Findings) != 2 {
		t.Fatalf("result = %+v", result)
	}
	if result.Dispute.Cost.RefereeSteps != 0 {
		t.Fatalf("referee steps = %d, want 0", result.Dispute.Cost.RefereeSteps)
	}
	for _, finding := range result.Findings {
		if finding.Verdict != protocol.VerdictBadState {
			t.Fatalf("finding = %+v", finding)
		}
	}
}

func TestBadWinnerFinalStatePreventsAcceptance(t *testing.T) {
	limits := protocol.DefaultLimits()
	_, honestServer := startWorker(t, "worker-a", limits, mutateFinalState)
	corrupt := newTraceWorker("worker-b", func(job.JobSpec) uint64 { return 2 }, 0)
	corruptServer := httptest.NewServer(corrupt)
	t.Cleanup(corruptServer.Close)
	client := mustCoordinator(t, honestServer.URL, corruptServer.URL, limits)

	result, err := client.Run(context.Background(), smallSpec(3))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != coordinator.Rejected || len(result.Findings) != 2 {
		t.Fatalf("result = %+v", result)
	}
	if result.Findings[0].Verdict != protocol.VerdictWrongStep ||
		result.Findings[1].Verdict != protocol.VerdictBadState {
		t.Fatalf("findings = %+v", result.Findings)
	}
}

func TestExpiredOpeningIsUnresolvedAndNeverBadOpening(t *testing.T) {
	limits := protocol.DefaultLimits()
	honest, honestServer := startWorker(t, "worker-a", limits, stallResponse(protocol.StepHashPath))
	corrupt := newTraceWorker("worker-b", func(job.JobSpec) uint64 { return 2 }, 0)
	corrupt.stallStepHash = true
	corruptServer := httptest.NewServer(corrupt)
	t.Cleanup(corruptServer.Close)
	config := coordinator.DefaultConfig()
	config.Limits = limits
	config.RequestTimeout = 40 * time.Millisecond
	config.JobTimeout = time.Second
	client := mustCoordinatorWithConfig(t, honestServer.URL, corruptServer.URL, config)

	result, err := client.Run(context.Background(), smallSpec(1))
	if err == nil {
		t.Fatal("Run() succeeded despite expired opening requests")
	}
	if len(result.Findings) != 0 {
		t.Fatalf("expired context produced verdicts: %+v", result.Findings)
	}
	if honest.ActiveAttempts() != 1 || corrupt.activeAttempts() != 1 {
		t.Fatal("unresolved opening deadline did not retain both attempts")
	}
}

type traceWorker struct {
	id                 string
	corruptAt          func(job.JobSpec) uint64
	parameterGroup     int
	badIndexedState    bool
	stallStepHash      bool
	mu                 sync.Mutex
	attempts           map[uint64]*traceAttempt
	firstOpenByAttempt map[uint64]uint64
}

type traceAttempt struct {
	machine *job.Machine
	states  [][]byte
	hashes  []protocol.Digest
	tree    *merkle.Tree
}

func newTraceWorker(id string, corruptAt func(job.JobSpec) uint64, parameterGroup int) *traceWorker {
	return &traceWorker{
		id: id, corruptAt: corruptAt, parameterGroup: parameterGroup,
		attempts: make(map[uint64]*traceAttempt), firstOpenByAttempt: make(map[uint64]uint64),
	}
}

func (worker *traceWorker) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	switch request.URL.Path {
	case protocol.ExecutePath:
		worker.execute(response, request)
	case protocol.StepHashPath:
		worker.stepHash(response, request)
	case protocol.StatePath:
		worker.state(response, request)
	case protocol.FinalStatePath:
		worker.finalState(response, request)
	case protocol.ClosePath:
		worker.close(response, request)
	default:
		http.NotFound(response, request)
	}
}

func (worker *traceWorker) execute(response http.ResponseWriter, request *http.Request) {
	var message protocol.ExecuteRequest
	decodeTestMessage(request, &message)
	spec, err := job.UnmarshalSpec(message.Job)
	if err != nil {
		panic(err)
	}
	machine, err := job.NewMachine(spec)
	if err != nil {
		panic(err)
	}
	corruptAt := worker.corruptAt(spec)
	state := machine.InitialState()
	states := make([][]byte, 0, machine.LeafCount())
	hashes := make([]protocol.Digest, 0, machine.LeafCount())
	for {
		if state.Step == corruptAt {
			flipStateParameter(&state, worker.parameterGroup)
		}
		encoded, err := machine.MarshalState(state)
		if err != nil {
			panic(err)
		}
		hash, err := machine.HashState(state)
		if err != nil {
			panic(err)
		}
		states = append(states, encoded)
		hashes = append(hashes, hash)
		if state.Step == spec.Steps {
			break
		}
		state, err = machine.Step(state)
		if err != nil {
			panic(err)
		}
	}
	tree, err := merkle.New(machine.ID(), machine.LeafCount(), hashes)
	if err != nil {
		panic(err)
	}
	worker.mu.Lock()
	worker.attempts[message.AttemptID] = &traceAttempt{machine: machine, states: states, hashes: hashes, tree: tree}
	worker.mu.Unlock()
	writeTestMessage(response, protocol.ExecuteResponse{
		Meta: message.Meta, WorkerID: worker.id, Root: protocol.EncodeDigest(tree.Root()),
		FinalStateHash: protocol.EncodeDigest(hashes[len(hashes)-1]),
	})
}

func (worker *traceWorker) stepHash(response http.ResponseWriter, request *http.Request) {
	var message protocol.StepHashRequest
	decodeTestMessage(request, &message)
	if worker.stallStepHash {
		<-request.Context().Done()
		return
	}
	worker.mu.Lock()
	attempt := worker.attempts[message.AttemptID]
	if _, exists := worker.firstOpenByAttempt[message.AttemptID]; !exists {
		worker.firstOpenByAttempt[message.AttemptID] = message.Index
	}
	worker.mu.Unlock()
	proof, err := attempt.tree.Proof(message.Index)
	if err != nil {
		panic(err)
	}
	siblings := make([]string, len(proof.Siblings))
	for i, sibling := range proof.Siblings {
		siblings[i] = protocol.EncodeDigest(sibling)
	}
	writeTestMessage(response, protocol.StepHashResponse{
		Meta: message.Meta, WorkerID: worker.id, Hash: protocol.EncodeDigest(attempt.hashes[message.Index]),
		Proof: protocol.InclusionProof{Index: message.Index, Siblings: siblings},
	})
}

func (worker *traceWorker) state(response http.ResponseWriter, request *http.Request) {
	var message protocol.StateRequest
	decodeTestMessage(request, &message)
	worker.mu.Lock()
	attempt := worker.attempts[message.AttemptID]
	worker.mu.Unlock()
	state := append([]byte(nil), attempt.states[message.Index]...)
	if worker.badIndexedState {
		state[0] ^= 1
	}
	writeTestMessage(response, protocol.StateResponse{
		Meta: message.Meta, WorkerID: worker.id, Index: message.Index, State: state,
	})
}

func (worker *traceWorker) finalState(response http.ResponseWriter, request *http.Request) {
	var message protocol.FinalStateRequest
	decodeTestMessage(request, &message)
	worker.mu.Lock()
	attempt := worker.attempts[message.AttemptID]
	worker.mu.Unlock()
	writeTestMessage(response, protocol.FinalStateResponse{
		Meta: message.Meta, WorkerID: worker.id, State: attempt.states[len(attempt.states)-1],
	})
}

func (worker *traceWorker) close(response http.ResponseWriter, request *http.Request) {
	var message protocol.CloseRequest
	decodeTestMessage(request, &message)
	worker.mu.Lock()
	delete(worker.attempts, message.AttemptID)
	worker.mu.Unlock()
	writeTestMessage(response, protocol.CloseResponse{Meta: message.Meta, WorkerID: worker.id, Closed: true})
}

func (worker *traceWorker) firstOpening(attemptID uint64) (uint64, bool) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	index, ok := worker.firstOpenByAttempt[attemptID]
	return index, ok
}

func (worker *traceWorker) activeAttempts() int {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return len(worker.attempts)
}

func flipStateParameter(state *job.State, group int) {
	var target *float32
	if group == 0 {
		target = &state.Parameters.W1[0]
	} else {
		target = &state.Parameters.W2[0]
	}
	*target = math.Float32frombits(math.Float32bits(*target) ^ 0x80000000)
}

func decodeTestMessage(request *http.Request, destination any) {
	if err := json.NewDecoder(request.Body).Decode(destination); err != nil {
		panic(err)
	}
}

func writeTestMessage(response http.ResponseWriter, message any) {
	response.Header().Set("Content-Type", protocol.JSONContentType)
	if err := json.NewEncoder(response).Encode(message); err != nil {
		panic(err)
	}
}

func mutateNthStepHashProof(next http.Handler, target uint64) http.Handler {
	var calls atomic.Uint64
	return mutateResponse(next, protocol.StepHashPath, func(response any) {
		if calls.Add(1) != target {
			return
		}
		message := response.(*protocol.StepHashResponse)
		message.Proof.Siblings[0] = flipHex(message.Proof.Siblings[0])
	})
}
