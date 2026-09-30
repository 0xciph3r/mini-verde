package job

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/0xciph3r/mini-verde/internal/repops"
)

const (
	// ProtocolVersion identifies the canonical job semantics implemented here.
	ProtocolVersion uint16 = 1

	// DefaultLearningRate is exactly 2^-8.
	DefaultLearningRate float32 = 0x1p-8

	initializationScale float32 = 0x1p-36
)

var (
	ErrInvalidJob   = errors.New("invalid job")
	ErrInvalidState = errors.New("invalid state")
	ErrNonFinite    = errors.New("non-finite float32")
	ErrJobComplete  = errors.New("job is complete")
)

// Dataset stores examples in row-major order. Inputs has Examples*InputSize
// values and Targets has Examples*OutputSize values.
type Dataset struct {
	Examples uint32    `json:"examples"`
	Inputs   []float32 `json:"inputs"`
	Targets  []float32 `json:"targets"`
}

// JobSpec contains every value that affects canonical execution.
type JobSpec struct {
	Version      uint16  `json:"version"`
	Seed         uint64  `json:"seed"`
	Steps        uint64  `json:"steps"`
	InputSize    uint32  `json:"input_size"`
	HiddenSize   uint32  `json:"hidden_size"`
	OutputSize   uint32  `json:"output_size"`
	BatchSize    uint32  `json:"batch_size"`
	LearningRate float32 `json:"learning_rate"`
	Dataset      Dataset `json:"dataset"`
}

// Parameters are stored in canonical W1, B1, W2, B2 order. Matrices are
// row-major.
type Parameters struct {
	W1 []float32
	B1 []float32
	W2 []float32
	B2 []float32
}

// State is the complete mutable state of the canonical machine.
type State struct {
	Step       uint64
	Parameters Parameters
}

// Machine is an immutable, validated job ready for canonical execution.
type Machine struct {
	spec       JobSpec
	id         [sha256.Size]byte
	w1Count    int
	b1Count    int
	w2Count    int
	b2Count    int
	paramCount int
}

// NewMachine validates and privately copies spec.
func NewMachine(spec JobSpec) (*Machine, error) {
	counts, err := validateSpec(spec)
	if err != nil {
		return nil, err
	}

	owned := spec
	owned.Dataset.Inputs = slices.Clone(spec.Dataset.Inputs)
	owned.Dataset.Targets = slices.Clone(spec.Dataset.Targets)
	encoded := encodeJobSpec(owned)
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("mini-verde/job/v1\x00"))
	_, _ = hasher.Write(encoded)
	var id [sha256.Size]byte
	copy(id[:], hasher.Sum(nil))

	return &Machine{
		spec:       owned,
		id:         id,
		w1Count:    counts[0],
		b1Count:    counts[1],
		w2Count:    counts[2],
		b2Count:    counts[3],
		paramCount: counts[0] + counts[1] + counts[2] + counts[3],
	}, nil
}

// ID returns the content-derived job identifier.
func (m *Machine) ID() [sha256.Size]byte {
	return m.id
}

// LeafCount is the trusted number of committed states: the initial state and
// one post-state for every job step.
func (m *Machine) LeafCount() uint64 {
	return m.spec.Steps + 1
}

// InitialState deterministically derives weights from the job seed and sets
// all biases to zero.
func (m *Machine) InitialState() State {
	backing := make([]float32, m.paramCount)
	parameters := m.parametersFrom(backing)
	for i := range parameters.W1 {
		parameters.W1[i] = initialWeight(m.spec.Seed, uint64(i))
	}
	w2Offset := uint64(m.w1Count + m.b1Count)
	for i := range parameters.W2 {
		parameters.W2[i] = initialWeight(m.spec.Seed, w2Offset+uint64(i))
	}
	return State{Parameters: parameters}
}

func initialWeight(seed, parameterIndex uint64) float32 {
	var material [len("mini-verde/init/v1\x00") + 16]byte
	copy(material[:], "mini-verde/init/v1\x00")
	offset := len("mini-verde/init/v1\x00")
	binary.LittleEndian.PutUint64(material[offset:offset+8], seed)
	binary.LittleEndian.PutUint64(material[offset+8:], parameterIndex)
	digest := sha256.Sum256(material[:])
	signed := int32(binary.LittleEndian.Uint32(digest[:4]))
	return float32(float32(signed) * initializationScale)
}

func validateSpec(spec JobSpec) ([4]int, error) {
	var counts [4]int
	if spec.Version != ProtocolVersion {
		return counts, fmt.Errorf("%w: protocol version %d, want %d", ErrInvalidJob, spec.Version, ProtocolVersion)
	}
	if spec.Steps == 0 {
		return counts, fmt.Errorf("%w: steps must be positive", ErrInvalidJob)
	}
	if spec.Steps == math.MaxUint64 {
		return counts, fmt.Errorf("%w: steps + initial state overflows leaf count", ErrInvalidJob)
	}
	if spec.InputSize == 0 || spec.HiddenSize == 0 || spec.OutputSize == 0 {
		return counts, fmt.Errorf("%w: model dimensions must be positive", ErrInvalidJob)
	}
	if spec.Dataset.Examples == 0 {
		return counts, fmt.Errorf("%w: dataset must contain an example", ErrInvalidJob)
	}
	if spec.BatchSize == 0 || spec.BatchSize > spec.Dataset.Examples {
		return counts, fmt.Errorf("%w: batch size %d outside [1,%d]", ErrInvalidJob, spec.BatchSize, spec.Dataset.Examples)
	}
	if !repops.IsFinite(spec.LearningRate) || spec.LearningRate <= 0 {
		return counts, fmt.Errorf("%w: learning rate must be finite and positive", ErrInvalidJob)
	}

	w1 := uint64(spec.HiddenSize) * uint64(spec.InputSize)
	b1 := uint64(spec.HiddenSize)
	w2 := uint64(spec.OutputSize) * uint64(spec.HiddenSize)
	b2 := uint64(spec.OutputSize)
	maxInt := uint64(^uint(0) >> 1)
	parameterCount, ok := checkedSum(w1, b1, w2, b2)
	if !ok || parameterCount > maxInt {
		return counts, fmt.Errorf("%w: parameter count overflows int", ErrInvalidJob)
	}
	scratchCount := 3*uint64(spec.HiddenSize) + 2*uint64(spec.OutputSize)
	if scratchCount > maxInt || parameterCount > (maxInt-scratchCount)/2 {
		return counts, fmt.Errorf("%w: step workspace overflows int", ErrInvalidJob)
	}
	inputCount := uint64(spec.Dataset.Examples) * uint64(spec.InputSize)
	targetCount := uint64(spec.Dataset.Examples) * uint64(spec.OutputSize)
	if inputCount != uint64(len(spec.Dataset.Inputs)) {
		return counts, fmt.Errorf("%w: got %d inputs, want %d", ErrInvalidJob, len(spec.Dataset.Inputs), inputCount)
	}
	if targetCount != uint64(len(spec.Dataset.Targets)) {
		return counts, fmt.Errorf("%w: got %d targets, want %d", ErrInvalidJob, len(spec.Dataset.Targets), targetCount)
	}
	floatCount, ok := checkedSum(inputCount, targetCount)
	const jobEncodingFixedBytes = uint64(2 + 8 + 8 + 5*4 + 4 + 2*8)
	if !ok || floatCount > (maxInt-jobEncodingFixedBytes)/4 {
		return counts, fmt.Errorf("%w: canonical job encoding overflows int", ErrInvalidJob)
	}
	const stateEncodingFixedBytes = uint64(4 + 2 + sha256.Size + 8 + 4*8)
	if parameterCount > (maxInt-stateEncodingFixedBytes)/4 {
		return counts, fmt.Errorf("%w: canonical state encoding overflows int", ErrInvalidJob)
	}
	for i, value := range spec.Dataset.Inputs {
		if !repops.IsFinite(value) {
			return counts, fmt.Errorf("%w: input %d: %w", ErrInvalidJob, i, ErrNonFinite)
		}
	}
	for i, value := range spec.Dataset.Targets {
		if !repops.IsFinite(value) {
			return counts, fmt.Errorf("%w: target %d: %w", ErrInvalidJob, i, ErrNonFinite)
		}
	}
	counts = [4]int{int(w1), int(b1), int(w2), int(b2)}
	return counts, nil
}

func checkedSum(values ...uint64) (uint64, bool) {
	var sum uint64
	for _, value := range values {
		if math.MaxUint64-sum < value {
			return 0, false
		}
		sum += value
	}
	return sum, true
}

func (m *Machine) parametersFrom(backing []float32) Parameters {
	w1End := m.w1Count
	b1End := w1End + m.b1Count
	w2End := b1End + m.w2Count
	b2End := w2End + m.b2Count
	return Parameters{
		W1: backing[:w1End:w1End],
		B1: backing[w1End:b1End:b1End],
		W2: backing[b1End:w2End:w2End],
		B2: backing[w2End:b2End:b2End],
	}
}

func (m *Machine) validateState(state State) error {
	if state.Step > m.spec.Steps {
		return fmt.Errorf("%w: step %d exceeds job length %d", ErrInvalidState, state.Step, m.spec.Steps)
	}
	parameters := state.Parameters
	if len(parameters.W1) != m.w1Count || len(parameters.B1) != m.b1Count ||
		len(parameters.W2) != m.w2Count || len(parameters.B2) != m.b2Count {
		return fmt.Errorf("%w: parameter shape does not match job", ErrInvalidState)
	}
	for _, group := range [][]float32{parameters.W1, parameters.B1, parameters.W2, parameters.B2} {
		for _, value := range group {
			if !repops.IsFinite(value) {
				return fmt.Errorf("%w: %w", ErrInvalidState, ErrNonFinite)
			}
		}
	}
	return nil
}

func encodeJobSpec(spec JobSpec) []byte {
	capacity := 2 + 8 + 8 + 5*4 + 4 + 2*8 + 4*(len(spec.Dataset.Inputs)+len(spec.Dataset.Targets))
	encoded := make([]byte, 0, capacity)
	encoded = appendUint16(encoded, spec.Version)
	encoded = appendUint64(encoded, spec.Seed)
	encoded = appendUint64(encoded, spec.Steps)
	encoded = appendUint32(encoded, spec.InputSize)
	encoded = appendUint32(encoded, spec.HiddenSize)
	encoded = appendUint32(encoded, spec.OutputSize)
	encoded = appendUint32(encoded, spec.BatchSize)
	encoded = appendUint32(encoded, spec.Dataset.Examples)
	encoded = appendUint32(encoded, math.Float32bits(spec.LearningRate))
	encoded = appendUint64(encoded, uint64(len(spec.Dataset.Inputs)))
	for _, value := range spec.Dataset.Inputs {
		encoded = appendUint32(encoded, math.Float32bits(value))
	}
	encoded = appendUint64(encoded, uint64(len(spec.Dataset.Targets)))
	for _, value := range spec.Dataset.Targets {
		encoded = appendUint32(encoded, math.Float32bits(value))
	}
	return encoded
}

func appendUint16(dst []byte, value uint16) []byte {
	return binary.LittleEndian.AppendUint16(dst, value)
}

func appendUint32(dst []byte, value uint32) []byte {
	return binary.LittleEndian.AppendUint32(dst, value)
}

func appendUint64(dst []byte, value uint64) []byte {
	return binary.LittleEndian.AppendUint64(dst, value)
}
