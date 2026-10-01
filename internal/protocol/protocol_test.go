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

func TestLimitsRejectBatchWorkAndRetainedStateBudgets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		limits func(Limits) Limits
	}{
		{
			name: "batch",
			limits: func(limits Limits) Limits {
				limits.MaxBatchSize = 1
				return limits
			},
		},
		{
			name: "total work",
			limits: func(limits Limits) Limits {
				limits.MaxWork = 1
				return limits
			},
		},
		{
			name: "retained states",
			limits: func(limits Limits) Limits {
				limits.MaxRetainedStateBytes = 1
				return limits
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limits := tc.limits(DefaultLimits())
			spec := JobSpecForTest()
			if tc.name == "batch" {
				spec.BatchSize = 2
				spec.Dataset.Examples = 2
				spec.Dataset.Inputs = []float32{0.5, -0.5}
				spec.Dataset.Targets = []float32{0.25, -0.25}
			}
			if err := limits.ValidateJob(spec); err == nil {
				t.Fatalf("ValidateJob() accepted a job above the %s limit", tc.name)
			}
		})
	}
}

func TestRetainedStateBudgetChargesFinalStateSeparately(t *testing.T) {
	t.Parallel()

	spec := JobSpecForTest()
	spec.Steps = CheckpointInterval
	// Four parameters and 78 fixed bytes make a 94-byte canonical state.
	// Steps 0 and 32 are checkpoints; this budget fits those two but not the
	// separately retained final state.
	limits := DefaultLimits()
	limits.MaxRetainedStateBytes = 2 * 94
	if err := limits.ValidateJob(spec); err == nil {
		t.Fatal("ValidateJob() did not charge the separately retained final state")
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
