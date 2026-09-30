package job

import (
	"math"
	"slices"
	"testing"
)

func TestCanonicalSpecRoundTripPreservesFloatBits(t *testing.T) {
	t.Parallel()

	spec := JobSpec{
		Version: ProtocolVersion, Seed: 99, Steps: 3,
		InputSize: 2, HiddenSize: 2, OutputSize: 1,
		BatchSize: 1, LearningRate: DefaultLearningRate,
		Dataset: Dataset{
			Examples: 2,
			Inputs: []float32{
				math.Float32frombits(0x80000000), 0.5,
				-0.25, math.Float32frombits(0x00000001),
			},
			Targets: []float32{0.125, -0.5},
		},
	}
	encoded, err := MarshalSpec(spec)
	if err != nil {
		t.Fatalf("MarshalSpec() error = %v", err)
	}
	decoded, err := UnmarshalSpec(encoded)
	if err != nil {
		t.Fatalf("UnmarshalSpec() error = %v", err)
	}
	reencoded, err := MarshalSpec(decoded)
	if err != nil {
		t.Fatalf("MarshalSpec(decoded) error = %v", err)
	}
	if !slices.Equal(reencoded, encoded) {
		t.Fatal("canonical job bytes changed after round trip")
	}
	originalMachine := mustMachine(t, spec)
	decodedMachine := mustMachine(t, decoded)
	if originalMachine.ID() != decodedMachine.ID() {
		t.Fatal("job ID changed after canonical round trip")
	}
	for i := range spec.Dataset.Inputs {
		if math.Float32bits(decoded.Dataset.Inputs[i]) != math.Float32bits(spec.Dataset.Inputs[i]) {
			t.Fatalf("input %d bits changed", i)
		}
	}
}

func TestCanonicalSpecDecoderRejectsMalformedData(t *testing.T) {
	t.Parallel()

	spec := syntheticLinearSpec(2, 2, 1, 2, 1, 1)
	encoded, err := MarshalSpec(spec)
	if err != nil {
		t.Fatalf("MarshalSpec() error = %v", err)
	}
	for name, malformed := range map[string][]byte{
		"empty":     nil,
		"truncated": slices.Clone(encoded[:len(encoded)-1]),
		"trailing":  append(slices.Clone(encoded), 0),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := UnmarshalSpec(malformed); err == nil {
				t.Fatal("UnmarshalSpec() accepted malformed bytes")
			}
		})
	}
}
