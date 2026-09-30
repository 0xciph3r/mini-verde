package job

import (
	"errors"
	"fmt"

	"github.com/0xciph3r/mini-verde/internal/repops"
)

// Step advances current by one canonical mini-batch SGD transition. It does
// not mutate current and allocates one backing workspace for the returned
// state, gradients, and per-example scratch values.
func (m *Machine) Step(current State) (State, error) {
	if err := m.validateState(current); err != nil {
		return State{}, err
	}
	if current.Step == m.spec.Steps {
		return State{}, ErrJobComplete
	}

	hiddenSize := int(m.spec.HiddenSize)
	outputSize := int(m.spec.OutputSize)
	scratchCount := 3*hiddenSize + 2*outputSize
	workspace := make([]float32, 2*m.paramCount+scratchCount)
	nextParameters := m.parametersFrom(workspace[:m.paramCount])
	gradients := m.parametersFrom(workspace[m.paramCount : 2*m.paramCount])
	offset := 2 * m.paramCount
	z1 := workspace[offset : offset+hiddenSize]
	offset += hiddenSize
	hidden := workspace[offset : offset+hiddenSize]
	offset += hiddenSize
	delta1 := workspace[offset : offset+hiddenSize]
	offset += hiddenSize
	prediction := workspace[offset : offset+outputSize]
	offset += outputSize
	delta2 := workspace[offset : offset+outputSize]

	inputSize := int(m.spec.InputSize)
	batchSize := int(m.spec.BatchSize)
	examples := uint64(m.spec.Dataset.Examples)
	batchBase := ((current.Step % examples) * uint64(m.spec.BatchSize)) % examples
	for batchIndex := 0; batchIndex < batchSize; batchIndex++ {
		example := (batchBase + uint64(batchIndex)) % examples
		inputOffset := int(example) * inputSize
		targetOffset := int(example) * outputSize
		x := m.spec.Dataset.Inputs[inputOffset : inputOffset+inputSize]
		target := m.spec.Dataset.Targets[targetOffset : targetOffset+outputSize]

		for h := 0; h < hiddenSize; h++ {
			sum := current.Parameters.B1[h]
			row := h * inputSize
			for i := 0; i < inputSize; i++ {
				sum = repops.MulAdd(sum, current.Parameters.W1[row+i], x[i])
			}
			z1[h] = sum
			if sum > 0 {
				hidden[h] = sum
			} else {
				hidden[h] = 0
			}
		}

		for o := 0; o < outputSize; o++ {
			sum := current.Parameters.B2[o]
			row := o * hiddenSize
			for h := 0; h < hiddenSize; h++ {
				sum = repops.MulAdd(sum, current.Parameters.W2[row+h], hidden[h])
			}
			prediction[o] = sum
		}

		for o := 0; o < outputSize; o++ {
			delta2[o] = repops.Sub(prediction[o], target[o])
		}

		for h := 0; h < hiddenSize; h++ {
			delta1[h] = 0
			if z1[h] > 0 {
				for o := 0; o < outputSize; o++ {
					delta1[h] = repops.MulAdd(delta1[h], current.Parameters.W2[o*hiddenSize+h], delta2[o])
				}
			}
		}

		for h := 0; h < hiddenSize; h++ {
			row := h * inputSize
			for i := 0; i < inputSize; i++ {
				gradients.W1[row+i] = repops.MulAdd(gradients.W1[row+i], delta1[h], x[i])
			}
		}
		for h := 0; h < hiddenSize; h++ {
			gradients.B1[h] = repops.Add(gradients.B1[h], delta1[h])
		}
		for o := 0; o < outputSize; o++ {
			row := o * hiddenSize
			for h := 0; h < hiddenSize; h++ {
				gradients.W2[row+h] = repops.MulAdd(gradients.W2[row+h], delta2[o], hidden[h])
			}
		}
		for o := 0; o < outputSize; o++ {
			gradients.B2[o] = repops.Add(gradients.B2[o], delta2[o])
		}
	}

	batchDivisor := float32(m.spec.BatchSize)
	updateGroup(nextParameters.W1, current.Parameters.W1, gradients.W1, batchDivisor, m.spec.LearningRate)
	updateGroup(nextParameters.B1, current.Parameters.B1, gradients.B1, batchDivisor, m.spec.LearningRate)
	updateGroup(nextParameters.W2, current.Parameters.W2, gradients.W2, batchDivisor, m.spec.LearningRate)
	updateGroup(nextParameters.B2, current.Parameters.B2, gradients.B2, batchDivisor, m.spec.LearningRate)

	next := State{Step: current.Step + 1, Parameters: nextParameters}
	if err := m.validateState(next); err != nil {
		if errors.Is(err, ErrNonFinite) {
			return State{}, fmt.Errorf("step %d: %w", next.Step, ErrNonFinite)
		}
		return State{}, err
	}
	return next, nil
}

func updateGroup(dst, values, gradientSums []float32, batchSize, learningRate float32) {
	for i := range dst {
		dst[i] = repops.SGD(values[i], gradientSums[i], batchSize, learningRate)
	}
}

// Loss returns mean half-squared error over the explicit dataset. It is
// diagnostic only and never enters committed state.
func (m *Machine) Loss(state State) (float32, error) {
	if err := m.validateState(state); err != nil {
		return 0, err
	}
	inputSize := int(m.spec.InputSize)
	hiddenSize := int(m.spec.HiddenSize)
	outputSize := int(m.spec.OutputSize)
	scratch := make([]float32, hiddenSize+outputSize)
	hidden := scratch[:hiddenSize]
	prediction := scratch[hiddenSize:]
	var loss float32
	for example := 0; example < int(m.spec.Dataset.Examples); example++ {
		x := m.spec.Dataset.Inputs[example*inputSize : (example+1)*inputSize]
		for h := 0; h < hiddenSize; h++ {
			sum := state.Parameters.B1[h]
			for i := 0; i < inputSize; i++ {
				sum = repops.MulAdd(sum, state.Parameters.W1[h*inputSize+i], x[i])
			}
			if sum > 0 {
				hidden[h] = sum
			} else {
				hidden[h] = 0
			}
		}
		for o := 0; o < outputSize; o++ {
			sum := state.Parameters.B2[o]
			for h := 0; h < hiddenSize; h++ {
				sum = repops.MulAdd(sum, state.Parameters.W2[o*hiddenSize+h], hidden[h])
			}
			prediction[o] = sum
			delta := repops.Sub(prediction[o], m.spec.Dataset.Targets[example*outputSize+o])
			term := repops.Mul(float32(0.5), repops.Mul(delta, delta))
			loss = repops.Add(loss, term)
		}
	}
	loss = repops.Div(loss, float32(m.spec.Dataset.Examples))
	if !repops.IsFinite(loss) {
		return 0, ErrNonFinite
	}
	return loss, nil
}
