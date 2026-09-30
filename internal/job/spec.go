package job

import (
	"bytes"
	"fmt"
	"math"
)

// MarshalSpec returns the canonical binary representation used to derive a
// job ID and transport the exact job between coordinator and workers.
func MarshalSpec(spec JobSpec) ([]byte, error) {
	if _, err := validateSpec(spec); err != nil {
		return nil, err
	}
	return encodeJobSpec(spec), nil
}

// UnmarshalSpec decodes and validates one canonical binary job
// representation. Alternative encodings and trailing bytes are rejected.
func UnmarshalSpec(encoded []byte) (JobSpec, error) {
	reader := bytes.NewReader(encoded)
	var spec JobSpec
	var err error
	if spec.Version, err = readUint16(reader); err != nil {
		return JobSpec{}, malformedSpec("version", err)
	}
	if spec.Seed, err = readUint64(reader); err != nil {
		return JobSpec{}, malformedSpec("seed", err)
	}
	if spec.Steps, err = readUint64(reader); err != nil {
		return JobSpec{}, malformedSpec("steps", err)
	}
	if spec.InputSize, err = readUint32(reader); err != nil {
		return JobSpec{}, malformedSpec("input size", err)
	}
	if spec.HiddenSize, err = readUint32(reader); err != nil {
		return JobSpec{}, malformedSpec("hidden size", err)
	}
	if spec.OutputSize, err = readUint32(reader); err != nil {
		return JobSpec{}, malformedSpec("output size", err)
	}
	if spec.BatchSize, err = readUint32(reader); err != nil {
		return JobSpec{}, malformedSpec("batch size", err)
	}
	if spec.Dataset.Examples, err = readUint32(reader); err != nil {
		return JobSpec{}, malformedSpec("example count", err)
	}
	learningRateBits, err := readUint32(reader)
	if err != nil {
		return JobSpec{}, malformedSpec("learning rate", err)
	}
	spec.LearningRate = math.Float32frombits(learningRateBits)

	inputCount, err := readUint64(reader)
	if err != nil {
		return JobSpec{}, malformedSpec("input count", err)
	}
	expectedInputs := uint64(spec.Dataset.Examples) * uint64(spec.InputSize)
	if inputCount != expectedInputs {
		return JobSpec{}, fmt.Errorf("%w: encoded input count %d, want %d", ErrInvalidJob, inputCount, expectedInputs)
	}
	spec.Dataset.Inputs, err = readFloat32s(reader, inputCount)
	if err != nil {
		return JobSpec{}, malformedSpec("inputs", err)
	}

	targetCount, err := readUint64(reader)
	if err != nil {
		return JobSpec{}, malformedSpec("target count", err)
	}
	expectedTargets := uint64(spec.Dataset.Examples) * uint64(spec.OutputSize)
	if targetCount != expectedTargets {
		return JobSpec{}, fmt.Errorf("%w: encoded target count %d, want %d", ErrInvalidJob, targetCount, expectedTargets)
	}
	spec.Dataset.Targets, err = readFloat32s(reader, targetCount)
	if err != nil {
		return JobSpec{}, malformedSpec("targets", err)
	}
	if reader.Len() != 0 {
		return JobSpec{}, fmt.Errorf("%w: trailing canonical job bytes", ErrInvalidJob)
	}
	if _, err := validateSpec(spec); err != nil {
		return JobSpec{}, err
	}
	canonical := encodeJobSpec(spec)
	if !bytes.Equal(canonical, encoded) {
		return JobSpec{}, fmt.Errorf("%w: non-canonical job encoding", ErrInvalidJob)
	}
	return spec, nil
}

func readFloat32s(reader *bytes.Reader, count uint64) ([]float32, error) {
	maxInt := uint64(^uint(0) >> 1)
	if count > maxInt || count > uint64(reader.Len())/4 {
		return nil, fmt.Errorf("float32 vector length %d exceeds remaining input", count)
	}
	values := make([]float32, int(count))
	for i := range values {
		bits, err := readUint32(reader)
		if err != nil {
			return nil, err
		}
		values[i] = math.Float32frombits(bits)
	}
	return values, nil
}

func malformedSpec(field string, err error) error {
	return fmt.Errorf("%w: malformed %s: %v", ErrInvalidJob, field, err)
}
