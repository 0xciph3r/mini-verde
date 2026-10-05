package coordinator

import (
	"context"
	"testing"

	"github.com/0xciph3r/mini-verde/internal/job"
)

func TestQuorumVerifierRequiresTwoMatchingIndependentCandidates(t *testing.T) {
	spec := job.JobSpec{
		Version: job.ProtocolVersion, Seed: 3, Steps: 1,
		InputSize: 1, HiddenSize: 1, OutputSize: 1,
		BatchSize: 1, LearningRate: job.DefaultLearningRate,
		Dataset: job.Dataset{Examples: 1, Inputs: []float32{0.5}, Targets: []float32{0.25}},
	}
	machine, err := job.NewMachine(spec)
	if err != nil {
		t.Fatal(err)
	}
	state := machine.InitialState()
	state, err = machine.Step(state)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := machine.HashState(state)
	if err != nil {
		t.Fatal(err)
	}
	verifier := QuorumVerifier{}
	if index, err := verifier.Verify(context.Background(), machine, []Candidate{
		{WorkerID: "a", State: state, Hash: hash},
		{WorkerID: "b", State: state, Hash: hash},
		{WorkerID: "c", State: machine.InitialState(), Hash: mustHash(t, machine, machine.InitialState())},
	}); err != nil || index > 1 {
		t.Fatalf("quorum verification = index %d, error %v", index, err)
	}
}

func mustHash(t *testing.T, machine *job.Machine, state job.State) [32]byte {
	t.Helper()
	hash, err := machine.HashState(state)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}
