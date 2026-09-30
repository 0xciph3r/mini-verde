package protocol

import (
	"math"
	"strings"
	"testing"

	"github.com/0xciph3r/mini-verde/internal/job"
)

func TestDigestTextRoundTripIsStrict(t *testing.T) {
	t.Parallel()

	var digest Digest
	for i := range digest {
		digest[i] = byte(i)
	}
	text := EncodeDigest(digest)
	if len(text) != 64 {
		t.Fatalf("encoded digest length = %d, want 64", len(text))
	}
	decoded, err := DecodeDigest(text)
	if err != nil {
		t.Fatalf("DecodeDigest() error = %v", err)
	}
	if decoded != digest {
		t.Fatal("digest changed after text round trip")
	}
	for _, malformed := range []string{"", text[:63], text + "00", strings.Repeat("z", 64)} {
		if _, err := DecodeDigest(malformed); err == nil {
			t.Fatalf("DecodeDigest(%q) accepted malformed input", malformed)
		}
	}
}

func TestDefaultLimitsRejectOversizedJobs(t *testing.T) {
	t.Parallel()

	limits := DefaultLimits()
	spec := JobSpecForTest()
	if err := limits.ValidateJob(spec); err != nil {
		t.Fatalf("ValidateJob(valid) error = %v", err)
	}
	spec.Steps = limits.MaxSteps + 1
	if err := limits.ValidateJob(spec); err == nil {
		t.Fatal("ValidateJob() accepted too many steps")
	}
}

func JobSpecForTest() job.JobSpec {
	return job.JobSpec{
		Version: job.ProtocolVersion, Seed: 1, Steps: 2,
		InputSize: 1, HiddenSize: 1, OutputSize: 1,
		BatchSize: 1, LearningRate: job.DefaultLearningRate,
		Dataset: job.Dataset{Examples: 1, Inputs: []float32{math.SmallestNonzeroFloat32}, Targets: []float32{0.25}},
	}
}
