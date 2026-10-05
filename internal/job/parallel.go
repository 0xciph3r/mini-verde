package job

import (
	"fmt"
	"sync"

	"github.com/0xciph3r/mini-verde/internal/repops"
)

// DefaultParallelChunkSize bounds the number of per-example gradients held by
// one parallel reduction chunk. The current admission limit is smaller, but
// keeping the chunk explicit prevents a future large-batch job from turning
// determinism into an unbounded allocation.
const DefaultParallelChunkSize = 10_000

// StepParallel computes one canonical step with concurrent per-example
// gradients and a fixed example-index reduction. Physical scheduling can vary
// freely; the logical gradient order remains the same as Step.
func (m *Machine) StepParallel(current State, workers, chunkSize int) (State, error) {
	return m.stepParallel(current, workers, chunkSize, false)
}

// StepNaiveParallel is an intentionally unsafe baseline for the M6 experiment.
// It reduces worker-local sums in worker order, so changing the worker count
// changes the floating-point addition tree and can change the committed bits.
func (m *Machine) StepNaiveParallel(current State, workers int) (State, error) {
	return m.stepParallel(current, workers, DefaultParallelChunkSize, true)
}

func (m *Machine) stepParallel(current State, workers, chunkSize int, naive bool) (State, error) {
	if err := m.validateState(current); err != nil {
		return State{}, err
	}
	if current.Step == m.spec.Steps {
		return State{}, ErrJobComplete
	}
	if workers <= 0 {
		return State{}, fmt.Errorf("parallel workers must be positive")
	}
	if chunkSize <= 0 {
		return State{}, fmt.Errorf("parallel chunk size must be positive")
	}

	batchSize := int(m.spec.BatchSize)
	if workers > batchSize {
		workers = batchSize
	}
	if workers == 0 {
		return State{}, fmt.Errorf("parallel batch must be positive")
	}
	gradientBacking := make([]float32, m.paramCount)
	gradients := m.parametersFrom(gradientBacking)
	hiddenSize := int(m.spec.HiddenSize)
	outputSize := int(m.spec.OutputSize)
	scratchSize := 3*hiddenSize + 2*outputSize
	examples := uint64(m.spec.Dataset.Examples)
	batchBase := ((current.Step % examples) * uint64(m.spec.BatchSize)) % examples

	for chunkStart := 0; chunkStart < batchSize; chunkStart += chunkSize {
		chunkLen := minInt(chunkSize, batchSize-chunkStart)
		partialBacking := make([]float32, chunkLen*m.paramCount)
		semaphore := make(chan struct{}, workers)
		var group sync.WaitGroup
		group.Add(chunkLen)
		for offset := 0; offset < chunkLen; offset++ {
			offset := offset
			group.Go(func() {
				defer group.Done()
				semaphore <- struct{}{}
				defer func() { <-semaphore }()
				example := (batchBase + uint64(chunkStart+offset)) % examples
				partial := m.parametersFrom(partialBacking[offset*m.paramCount : (offset+1)*m.paramCount])
				scratch := make([]float32, scratchSize)
				accumulateExampleGradient(m, current, example, partial, scratch)
			})
		}
		group.Wait()
		if naive {
			// The baseline deliberately uses worker-local accumulation. The
			// assignment changes with worker count, and so does the rounding
			// tree. It is useful only as an experiment, never as protocol code.
			localBacking := make([]float32, workers*m.paramCount)
			locals := make([]Parameters, workers)
			for worker := range locals {
				locals[worker] = m.parametersFrom(localBacking[worker*m.paramCount : (worker+1)*m.paramCount])
			}
			for offset := 0; offset < chunkLen; offset++ {
				worker := offset % workers
				addParameters(locals[worker], m.parametersFrom(partialBacking[offset*m.paramCount:(offset+1)*m.paramCount]))
			}
			for worker := range locals {
				addParameters(gradients, locals[worker])
			}
		} else {
			// This is the canonical reduction: chunk-local example index, then
			// the fixed parameter order inside addParameters.
			for offset := 0; offset < chunkLen; offset++ {
				addParameters(gradients, m.parametersFrom(partialBacking[offset*m.paramCount:(offset+1)*m.paramCount]))
			}
		}
	}

	nextBacking := make([]float32, m.paramCount)
	nextParameters := m.parametersFrom(nextBacking)
	return m.advanceWithGradients(current, gradients, nextParameters)
}

func addParameters(dst, src Parameters) {
	for i := range dst.W1 {
		dst.W1[i] = repops.Add(dst.W1[i], src.W1[i])
	}
	for i := range dst.B1 {
		dst.B1[i] = repops.Add(dst.B1[i], src.B1[i])
	}
	for i := range dst.W2 {
		dst.W2[i] = repops.Add(dst.W2[i], src.W2[i])
	}
	for i := range dst.B2 {
		dst.B2[i] = repops.Add(dst.B2[i], src.B2[i])
	}
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}
