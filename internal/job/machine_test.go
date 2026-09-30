package job

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"os"
	"slices"
	"strings"
	"testing"
)

func TestStepMatchesHandDerivedBits(t *testing.T) {
	t.Parallel()

	spec := JobSpec{
		Version:      ProtocolVersion,
		Seed:         1,
		Steps:        1,
		InputSize:    2,
		HiddenSize:   2,
		OutputSize:   1,
		BatchSize:    1,
		LearningRate: 0.25,
		Dataset: Dataset{
			Examples: 1,
			Inputs:   []float32{1, 0.5},
			Targets:  []float32{0.125},
		},
	}
	machine := mustMachine(t, spec)
	state := State{
		Step: 0,
		Parameters: Parameters{
			W1: []float32{0.5, 0.25, -0.25, 0.5},
			B1: []float32{0, 0.25},
			W2: []float32{0.5, -0.25},
			B2: []float32{0.125},
		},
	}
	before := parameterBits(state.Parameters)

	next, err := machine.Step(state)
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}

	// Hand derivation:
	// hidden = [0.625, 0.25], prediction = 0.375, delta2 = 0.25,
	// delta1 = [0.125, -0.0625]. Applying learning rate 0.25 gives
	// these exact dyadic float32 values in W1, B1, W2, B2 order.
	want := []uint32{
		0x3ef00000, 0x3e700000, 0xbe700000, 0x3f020000,
		0xbd000000, 0x3e880000,
		0x3eec0000, 0xbe880000,
		0x3d800000,
	}
	if got := parameterBits(next.Parameters); !slices.Equal(got, want) {
		t.Fatalf("Step() parameter bits = %08x, want %08x", got, want)
	}
	if next.Step != 1 {
		t.Fatalf("Step() index = %d, want 1", next.Step)
	}
	if got := parameterBits(state.Parameters); !slices.Equal(got, before) {
		t.Fatalf("Step() mutated its input: got %08x, want %08x", got, before)
	}
}

func TestStepGradientAgainstFloat64FiniteDifferences(t *testing.T) {
	t.Parallel()

	const learningRate = float32(0x1p-10)
	spec := JobSpec{
		Version:      ProtocolVersion,
		Seed:         2,
		Steps:        1,
		InputSize:    2,
		HiddenSize:   2,
		OutputSize:   1,
		BatchSize:    2,
		LearningRate: learningRate,
		Dataset: Dataset{
			Examples: 2,
			Inputs:   []float32{0.5, -0.25, -0.25, 0.5},
			Targets:  []float32{0.125, -0.25},
		},
	}
	machine := mustMachine(t, spec)
	state := State{
		Parameters: Parameters{
			W1: []float32{0.5, -0.25, 0.25, 0.5},
			B1: []float32{0.75, 0.5},
			W2: []float32{0.5, -0.75},
			B2: []float32{0.25},
		},
	}
	next, err := machine.Step(state)
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}

	oldParameters := parameterValues(state.Parameters)
	newParameters := parameterValues(next.Parameters)
	const epsilon = 1e-4
	for i := range oldParameters {
		analytic := (float64(oldParameters[i]) - float64(newParameters[i])) / float64(learningRate)
		plus := float64Parameters(oldParameters)
		minus := float64Parameters(oldParameters)
		plus[i] += epsilon
		minus[i] -= epsilon
		numeric := (lossFloat64(spec, plus) - lossFloat64(spec, minus)) / (2 * epsilon)
		tolerance := 3e-4 + 2e-3*math.Abs(numeric)
		if math.Abs(analytic-numeric) > tolerance {
			t.Errorf("gradient[%d] = %.9g, finite difference = %.9g (tolerance %.3g)", i, analytic, numeric, tolerance)
		}
	}
}

func TestDefaultJobLossDecreasesForOneThousandSteps(t *testing.T) {
	spec := syntheticLinearSpec(32, 256, 8, 256, 8, 1_000)
	machine := mustMachine(t, spec)
	state := machine.InitialState()
	initialLoss, err := machine.Loss(state)
	if err != nil {
		t.Fatalf("initial Loss() error = %v", err)
	}

	for range spec.Steps {
		state, err = machine.Step(state)
		if err != nil {
			t.Fatalf("Step() error at step %d = %v", state.Step, err)
		}
	}
	finalLoss, err := machine.Loss(state)
	if err != nil {
		t.Fatalf("final Loss() error = %v", err)
	}
	if !finiteFloat32(initialLoss) || !finiteFloat32(finalLoss) {
		t.Fatalf("loss is not finite: initial=%v final=%v", initialLoss, finalLoss)
	}
	if finalLoss >= initialLoss {
		t.Fatalf("loss did not decrease: initial=%g final=%g", initialLoss, finalLoss)
	}
	t.Logf("loss decreased from %g to %g", initialLoss, finalLoss)
}

func TestRunsAndRestartProduceIdenticalHashes(t *testing.T) {
	t.Parallel()

	spec := syntheticLinearSpec(4, 8, 2, 16, 4, 32)
	machine := mustMachine(t, spec)
	first := trace(t, machine, machine.InitialState(), spec.Steps)
	second := trace(t, machine, machine.InitialState(), spec.Steps)
	if !slices.Equal(first, second) {
		t.Fatal("two fresh runs produced different hash traces")
	}

	state := machine.InitialState()
	for range uint64(13) {
		var err error
		state, err = machine.Step(state)
		if err != nil {
			t.Fatalf("Step() before restart error = %v", err)
		}
	}
	encoded, err := machine.MarshalState(state)
	if err != nil {
		t.Fatalf("MarshalState() error = %v", err)
	}
	restored, err := machine.UnmarshalState(encoded)
	if err != nil {
		t.Fatalf("UnmarshalState() error = %v", err)
	}
	afterRestart := trace(t, machine, restored, spec.Steps-restored.Step)
	if !slices.Equal(afterRestart, first[restored.Step:]) {
		t.Fatal("restart produced a different hash suffix")
	}
}

func TestStateEncodingPreservesBitsAndBindsHash(t *testing.T) {
	t.Parallel()

	spec := syntheticLinearSpec(2, 2, 1, 2, 1, 1)
	machine := mustMachine(t, spec)
	state := machine.InitialState()
	state.Parameters.W1[0] = math.Float32frombits(0x80000000) // negative zero
	encoded, err := machine.MarshalState(state)
	if err != nil {
		t.Fatalf("MarshalState() error = %v", err)
	}
	restored, err := machine.UnmarshalState(encoded)
	if err != nil {
		t.Fatalf("UnmarshalState() error = %v", err)
	}
	if got := math.Float32bits(restored.Parameters.W1[0]); got != 0x80000000 {
		t.Fatalf("negative zero bits = %08x, want 80000000", got)
	}

	originalHash, err := machine.HashState(state)
	if err != nil {
		t.Fatalf("HashState() error = %v", err)
	}
	changed := cloneState(state)
	changed.Parameters.W1[0] = math.Float32frombits(math.Float32bits(changed.Parameters.W1[0]) ^ 1)
	changedHash, err := machine.HashState(changed)
	if err != nil {
		t.Fatalf("HashState(changed) error = %v", err)
	}
	if originalHash == changedHash {
		t.Fatal("one-bit parameter change did not change state hash")
	}
}

func TestStateDecoderRejectsMalformedOrForeignData(t *testing.T) {
	t.Parallel()

	spec := syntheticLinearSpec(2, 2, 1, 2, 1, 1)
	machine := mustMachine(t, spec)
	encoded, err := machine.MarshalState(machine.InitialState())
	if err != nil {
		t.Fatalf("MarshalState() error = %v", err)
	}

	trailing := append(slices.Clone(encoded), 0)
	truncated := slices.Clone(encoded[:len(encoded)-1])
	nonFinite := slices.Clone(encoded)
	const firstW1Bits = 4 + 2 + sha256.Size + 8 + 8
	binary.LittleEndian.PutUint32(nonFinite[firstW1Bits:firstW1Bits+4], 0x7f800000)
	for name, malformed := range map[string][]byte{
		"trailing byte": trailing,
		"truncated":     truncated,
		"infinity":      nonFinite,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := machine.UnmarshalState(malformed); err == nil {
				t.Fatal("UnmarshalState() accepted malformed input")
			}
		})
	}

	foreignSpec := spec
	foreignSpec.Seed++
	foreign := mustMachine(t, foreignSpec)
	if _, err := foreign.UnmarshalState(encoded); err == nil {
		t.Fatal("UnmarshalState() accepted state from another job")
	}
}

var allocationSink State

func TestStepUsesOneAllocation(t *testing.T) {
	machine := mustMachine(t, syntheticLinearSpec(4, 8, 2, 16, 4, 1))
	initial := machine.InitialState()
	var stepErr error
	allocations := testing.AllocsPerRun(100, func() {
		allocationSink, stepErr = machine.Step(initial)
	})
	if stepErr != nil {
		t.Fatalf("Step() error = %v", stepErr)
	}
	if allocations != 1 {
		t.Fatalf("Step() allocations = %g, want 1", allocations)
	}
}

func TestRejectsNonFiniteValues(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		value float32
	}{
		{name: "NaN", value: float32(math.NaN())},
		{name: "positive infinity", value: float32(math.Inf(1))},
		{name: "negative infinity", value: float32(math.Inf(-1))},
	} {
		t.Run(tc.name+" in job", func(t *testing.T) {
			spec := syntheticLinearSpec(1, 1, 1, 1, 1, 1)
			spec.Dataset.Inputs[0] = tc.value
			if _, err := NewMachine(spec); err == nil {
				t.Fatal("NewMachine() accepted a non-finite dataset value")
			}
		})
	}

	spec := syntheticLinearSpec(1, 1, 1, 1, 1, 1)
	spec.Dataset.Inputs[0] = 2
	machine := mustMachine(t, spec)
	state := State{Parameters: Parameters{
		W1: []float32{math.MaxFloat32}, B1: []float32{0},
		W2: []float32{1}, B2: []float32{0},
	}}
	if _, err := machine.Step(state); err == nil {
		t.Fatal("Step() accepted a transition that overflowed to infinity")
	}
}

func TestRejectsMalformedShapesWithoutOverflow(t *testing.T) {
	t.Parallel()

	overflowingLeafCount := syntheticLinearSpec(1, 1, 1, 1, 1, math.MaxUint64)
	tests := []JobSpec{
		{},
		overflowingLeafCount,
		{
			Version: ProtocolVersion, Steps: 1,
			InputSize: math.MaxUint32, HiddenSize: math.MaxUint32,
			OutputSize: 1, BatchSize: 1, LearningRate: DefaultLearningRate,
			Dataset: Dataset{Examples: 1},
		},
	}
	for i, spec := range tests {
		if _, err := NewMachine(spec); err == nil {
			t.Errorf("NewMachine(test %d) accepted malformed shape", i)
		}
	}
}

func TestGoldenTrace(t *testing.T) {
	t.Parallel()

	const goldenSteps = 64
	spec := syntheticLinearSpec(4, 8, 2, 16, 4, goldenSteps)
	machine := mustMachine(t, spec)
	data, err := os.ReadFile("testdata/arm64_golden_trace.txt")
	if err != nil {
		if os.IsNotExist(err) {
			got := trace(t, machine, machine.InitialState(), spec.Steps)
			lines := make([]string, len(got))
			for i := range got {
				lines[i] = hex.EncodeToString(got[i][:])
			}
			t.Fatalf("golden trace is missing; generated arm64 trace:\n%s", strings.Join(lines, "\n"))
		}
		t.Fatalf("read golden trace: %v", err)
	}
	want := strings.Fields(string(data))
	if len(want) != goldenSteps {
		t.Fatalf("golden trace has %d hashes, want %d", len(want), goldenSteps)
	}
	got := trace(t, machine, machine.InitialState(), spec.Steps)
	if len(got) != len(want) {
		t.Fatalf("trace length = %d, want %d", len(got), len(want))
	}
	for i := range got {
		if hex.EncodeToString(got[i][:]) != want[i] {
			t.Fatalf("hash at step %d = %x, want %s", i+1, got[i], want[i])
		}
	}
}

func mustMachine(t *testing.T, spec JobSpec) *Machine {
	t.Helper()
	machine, err := NewMachine(spec)
	if err != nil {
		t.Fatalf("NewMachine() error = %v", err)
	}
	return machine
}

func trace(t *testing.T, machine *Machine, state State, steps uint64) [][32]byte {
	t.Helper()
	hashes := make([][32]byte, 0, steps)
	for range steps {
		var err error
		state, err = machine.Step(state)
		if err != nil {
			t.Fatalf("Step() at %d error = %v", state.Step, err)
		}
		hash, err := machine.HashState(state)
		if err != nil {
			t.Fatalf("HashState() at %d error = %v", state.Step, err)
		}
		hashes = append(hashes, hash)
	}
	return hashes
}

func syntheticLinearSpec(input, hidden, output, examples, batch uint32, steps uint64) JobSpec {
	inputs := make([]float32, uint64(input)*uint64(examples))
	targets := make([]float32, uint64(output)*uint64(examples))
	var counter uint64
	for example := range examples {
		for i := range input {
			var material [16]byte
			binary.LittleEndian.PutUint64(material[0:8], 0x6d696e6976657264)
			binary.LittleEndian.PutUint64(material[8:16], counter)
			digest := sha256.Sum256(material[:])
			raw := int32(binary.LittleEndian.Uint16(digest[:2])) - 32768
			inputs[uint64(example)*uint64(input)+uint64(i)] = float32(raw) * float32(0x1p-15)
			counter++
		}
	}
	for example := range examples {
		for o := range output {
			sum := float32(int32(o%5)-2) * float32(0x1p-7)
			for i := range input {
				coefficient := float32(int32((o*31+i*17)%17)-8) * float32(0x1p-6)
				x := inputs[uint64(example)*uint64(input)+uint64(i)]
				sum = float32(sum + float32(coefficient*x))
			}
			targets[uint64(example)*uint64(output)+uint64(o)] = sum
		}
	}
	return JobSpec{
		Version: ProtocolVersion, Seed: 42, Steps: steps,
		InputSize: input, HiddenSize: hidden, OutputSize: output,
		BatchSize: batch, LearningRate: DefaultLearningRate,
		Dataset: Dataset{Examples: examples, Inputs: inputs, Targets: targets},
	}
}

func parameterValues(parameters Parameters) []float32 {
	values := make([]float32, 0, len(parameters.W1)+len(parameters.B1)+len(parameters.W2)+len(parameters.B2))
	values = append(values, parameters.W1...)
	values = append(values, parameters.B1...)
	values = append(values, parameters.W2...)
	values = append(values, parameters.B2...)
	return values
}

func parameterBits(parameters Parameters) []uint32 {
	values := parameterValues(parameters)
	bits := make([]uint32, len(values))
	for i, value := range values {
		bits[i] = math.Float32bits(value)
	}
	return bits
}

func float64Parameters(parameters []float32) []float64 {
	result := make([]float64, len(parameters))
	for i, parameter := range parameters {
		result[i] = float64(parameter)
	}
	return result
}

func lossFloat64(spec JobSpec, parameters []float64) float64 {
	input := int(spec.InputSize)
	hidden := int(spec.HiddenSize)
	output := int(spec.OutputSize)
	w1End := hidden * input
	b1End := w1End + hidden
	w2End := b1End + output*hidden
	w1, b1 := parameters[:w1End], parameters[w1End:b1End]
	w2, b2 := parameters[b1End:w2End], parameters[w2End:]
	var loss float64
	activations := make([]float64, hidden)
	for example := range int(spec.Dataset.Examples) {
		x := spec.Dataset.Inputs[example*input : (example+1)*input]
		for h := range hidden {
			sum := b1[h]
			for i := range input {
				sum += w1[h*input+i] * float64(x[i])
			}
			activations[h] = max(sum, 0)
		}
		for o := range output {
			prediction := b2[o]
			for h := range hidden {
				prediction += w2[o*hidden+h] * activations[h]
			}
			delta := prediction - float64(spec.Dataset.Targets[example*output+o])
			loss += 0.5 * delta * delta
		}
	}
	return loss / float64(spec.Dataset.Examples)
}

func cloneState(state State) State {
	return State{Step: state.Step, Parameters: Parameters{
		W1: slices.Clone(state.Parameters.W1),
		B1: slices.Clone(state.Parameters.B1),
		W2: slices.Clone(state.Parameters.W2),
		B2: slices.Clone(state.Parameters.B2),
	}}
}

func finiteFloat32(value float32) bool {
	converted := float64(value)
	return !math.IsNaN(converted) && !math.IsInf(converted, 0)
}
