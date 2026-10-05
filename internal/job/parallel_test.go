package job

import (
	"slices"
	"testing"
)

func TestStepParallelPreservesCanonicalBitsAcrossWorkerCountsAndChunks(t *testing.T) {
	spec := syntheticLinearSpec(4, 8, 2, 37, 8, 12)
	machine := mustMachine(t, spec)
	state := machine.InitialState()
	canonical, err := machine.Step(state)
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	for _, tc := range []struct {
		workers int
		chunk   int
	}{
		{workers: 1, chunk: 1},
		{workers: 4, chunk: 3},
		{workers: 16, chunk: 10_000},
	} {
		got, err := machine.StepParallel(state, tc.workers, tc.chunk)
		if err != nil {
			t.Fatalf("StepParallel(%d, %d) error = %v", tc.workers, tc.chunk, err)
		}
		if got.Step != canonical.Step || !slices.Equal(parameterValues(got.Parameters), parameterValues(canonical.Parameters)) {
			t.Fatalf("StepParallel(%d, %d) changed canonical bits", tc.workers, tc.chunk)
		}
	}
}

func TestNaiveParallelReductionChangesBitsWithWorkerCount(t *testing.T) {
	spec := syntheticLinearSpec(3, 7, 2, 257, 256, 1)
	machine := mustMachine(t, spec)
	state := machine.InitialState()
	var traces [][]float32
	for _, workers := range []int{1, 4, 16} {
		next, err := machine.StepNaiveParallel(state, workers)
		if err != nil {
			t.Fatalf("StepNaiveParallel(%d) error = %v", workers, err)
		}
		traces = append(traces, parameterValues(next.Parameters))
	}
	if slices.Equal(traces[0], traces[1]) && slices.Equal(traces[1], traces[2]) {
		t.Fatal("naive worker-local reduction did not expose an order-dependent bit difference")
	}
}
