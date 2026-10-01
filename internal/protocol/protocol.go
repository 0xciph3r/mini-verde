// Package protocol defines Mini-Verde's HTTP wire messages and shared
// admission limits.
package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/0xciph3r/mini-verde/internal/job"
)

const (
	JSONContentType = "application/json"
	ExecutePath     = "/v1/execute"
	FinalStatePath  = "/v1/final-state"
	StepHashPath    = "/v1/step-hash"
	StatePath       = "/v1/state"
	ClosePath       = "/v1/close"

	VerdictWrongStep  = "WrongStep"
	VerdictBadOpening = "BadOpening"
	VerdictBadState   = "BadState"
	VerdictTimeout    = "Timeout"
	VerdictBothWrong  = "BothWrong"

	CheckpointInterval uint64 = 32
)

var (
	ErrInvalidDigest = errors.New("invalid digest")
	ErrInvalidMeta   = errors.New("invalid protocol metadata")
	ErrAdmission     = errors.New("job exceeds admission limits")
)

type Digest = [sha256.Size]byte

// Meta identifies one message in one worker attempt.
type Meta struct {
	JobID     string `json:"job_id"`
	AttemptID uint64 `json:"attempt_id"`
	Round     uint64 `json:"round"`
}

type ExecuteRequest struct {
	Meta
	Job []byte `json:"job"`
}

type ExecuteResponse struct {
	Meta
	WorkerID       string `json:"worker_id"`
	Root           string `json:"root"`
	FinalStateHash string `json:"final_state_hash"`
}

type FinalStateRequest struct {
	Meta
}

type FinalStateResponse struct {
	Meta
	WorkerID string `json:"worker_id"`
	State    []byte `json:"state"`
}

type InclusionProof struct {
	Index    uint64   `json:"index"`
	Siblings []string `json:"siblings"`
}

type StepHashRequest struct {
	Meta
	Index uint64 `json:"index"`
}

type StepHashResponse struct {
	Meta
	WorkerID string         `json:"worker_id"`
	Hash     string         `json:"hash"`
	Proof    InclusionProof `json:"proof"`
}

type StateRequest struct {
	Meta
	Index uint64 `json:"index"`
}

type StateResponse struct {
	Meta
	WorkerID string `json:"worker_id"`
	Index    uint64 `json:"index"`
	State    []byte `json:"state"`
}

type CloseRequest struct {
	Meta
}

type CloseResponse struct {
	Meta
	WorkerID string `json:"worker_id"`
	Closed   bool   `json:"closed"`
}

type ErrorResponse struct {
	Meta
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Limits bounds untrusted HTTP inputs and worker resource retention.
type Limits struct {
	MaxBodyBytes            int64
	MaxParameters           uint64
	MaxDatasetValues        uint64
	MaxBatchSize            uint64
	MaxSteps                uint64
	MaxWork                 uint64
	MaxRetainedStateBytes   uint64
	MaxConcurrentExecutions int
	MaxActiveAttempts       int
}

func DefaultLimits() Limits {
	return Limits{
		MaxBodyBytes:            8 << 20,
		MaxParameters:           100_000,
		MaxDatasetValues:        1_000_000,
		MaxBatchSize:            1_024,
		MaxSteps:                100_000,
		MaxWork:                 1_000_000_000,
		MaxRetainedStateBytes:   16 << 20,
		MaxConcurrentExecutions: 2,
		MaxActiveAttempts:       8,
	}
}

func (limits Limits) Validate() error {
	if limits.MaxBodyBytes <= 0 || limits.MaxParameters == 0 || limits.MaxDatasetValues == 0 ||
		limits.MaxBatchSize == 0 || limits.MaxSteps == 0 || limits.MaxWork == 0 ||
		limits.MaxRetainedStateBytes == 0 || limits.MaxConcurrentExecutions <= 0 || limits.MaxActiveAttempts <= 0 {
		return fmt.Errorf("%w: every limit must be positive", ErrAdmission)
	}
	return nil
}

func (limits Limits) ValidateJob(spec job.JobSpec) error {
	if err := limits.Validate(); err != nil {
		return err
	}
	if _, err := job.NewMachine(spec); err != nil {
		return err
	}
	if spec.Steps > limits.MaxSteps {
		return fmt.Errorf("%w: steps %d exceed %d", ErrAdmission, spec.Steps, limits.MaxSteps)
	}
	if uint64(spec.BatchSize) > limits.MaxBatchSize {
		return fmt.Errorf("%w: batch size %d exceeds %d", ErrAdmission, spec.BatchSize, limits.MaxBatchSize)
	}
	w1 := uint64(spec.HiddenSize) * uint64(spec.InputSize)
	w2 := uint64(spec.OutputSize) * uint64(spec.HiddenSize)
	parameters := w1 + uint64(spec.HiddenSize) + w2 + uint64(spec.OutputSize)
	if parameters > limits.MaxParameters {
		return fmt.Errorf("%w: parameters %d exceed %d", ErrAdmission, parameters, limits.MaxParameters)
	}
	datasetValues := uint64(len(spec.Dataset.Inputs)) + uint64(len(spec.Dataset.Targets))
	if datasetValues > limits.MaxDatasetValues {
		return fmt.Errorf("%w: dataset values %d exceed %d", ErrAdmission, datasetValues, limits.MaxDatasetValues)
	}
	if exceedsProductLimit(limits.MaxWork, spec.Steps, uint64(spec.BatchSize), parameters) {
		return fmt.Errorf(
			"%w: work steps(%d) * batch(%d) * parameters(%d) exceeds %d",
			ErrAdmission, spec.Steps, spec.BatchSize, parameters, limits.MaxWork,
		)
	}
	retainedBytes, ok := retainedStateBytes(spec.Steps, parameters)
	if !ok || retainedBytes > limits.MaxRetainedStateBytes {
		return fmt.Errorf("%w: retained states exceed %d bytes", ErrAdmission, limits.MaxRetainedStateBytes)
	}
	return nil
}

func retainedStateBytes(steps, parameters uint64) (uint64, bool) {
	const stateFixedBytes = uint64(4 + 2 + sha256.Size + 8 + 4*8)
	if parameters > (^uint64(0)-stateFixedBytes)/4 {
		return 0, false
	}
	stateBytes := stateFixedBytes + 4*parameters
	// State zero and every multiple of 32 are checkpoints. The final state is
	// retained separately and is charged even when it is also a checkpoint.
	stateCount := steps/CheckpointInterval + 2
	if stateBytes > ^uint64(0)/stateCount {
		return 0, false
	}
	return stateBytes * stateCount, true
}

func exceedsProductLimit(limit uint64, factors ...uint64) bool {
	product := uint64(1)
	for _, factor := range factors {
		if factor != 0 && product > limit/factor {
			return true
		}
		product *= factor
	}
	return product > limit
}

func EncodeDigest(digest Digest) string {
	return hex.EncodeToString(digest[:])
}

func DecodeDigest(value string) (Digest, error) {
	var digest Digest
	if len(value) != hex.EncodedLen(len(digest)) || value != strings.ToLower(value) {
		return digest, fmt.Errorf("%w: expected %d lowercase hexadecimal characters", ErrInvalidDigest, hex.EncodedLen(len(digest)))
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return digest, fmt.Errorf("%w: %v", ErrInvalidDigest, err)
	}
	copy(digest[:], decoded)
	return digest, nil
}

func ValidateMeta(meta Meta) (Digest, error) {
	digest, err := DecodeDigest(meta.JobID)
	if err != nil {
		return Digest{}, fmt.Errorf("%w: %v", ErrInvalidMeta, err)
	}
	if meta.AttemptID == 0 {
		return Digest{}, fmt.Errorf("%w: attempt ID must be positive", ErrInvalidMeta)
	}
	return digest, nil
}

func SameMeta(left, right Meta) bool {
	return left == right
}
