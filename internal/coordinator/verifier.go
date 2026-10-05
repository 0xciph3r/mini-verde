package coordinator

import (
	"bytes"
	"context"
	"fmt"

	"github.com/0xciph3r/mini-verde/internal/job"
	"github.com/0xciph3r/mini-verde/internal/protocol"
)

// Verifier independently checks a candidate result before the coordinator
// exposes it as accepted. The interface keeps the strict full-run policy
// replaceable by a future two-honest-of-three verifier without changing the
// worker protocol or acceptance plumbing.
type Verifier interface {
	Verify(context.Context, *job.Machine, []Candidate) (int, error)
}

// Candidate is a worker result presented to an acceptance policy. The worker
// identity is kept at this layer so a future quorum policy can require
// independently identified checkers rather than counting duplicate bytes.
type Candidate struct {
	WorkerID string
	State    job.State
	Hash     protocol.Digest
}

// StrictVerifier replays the canonical machine from the initial state to the
// candidate state. It is intentionally expensive: this mode makes the trusted
// coordinator an independent checker and therefore detects equal wrong
// results from colluding workers.
type StrictVerifier struct{}

func (StrictVerifier) Verify(ctx context.Context, machine *job.Machine, candidates []Candidate) (int, error) {
	if len(candidates) != 1 {
		return -1, fmt.Errorf("strict verifier requires exactly one candidate")
	}
	candidate := candidates[0]
	if err := verifyStrictCandidate(ctx, machine, candidate.State, candidate.Hash); err != nil {
		return -1, err
	}
	return 0, nil
}

func verifyStrictCandidate(ctx context.Context, machine *job.Machine, candidate job.State, wantHash protocol.Digest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if candidate.Step > machine.LeafCount()-1 {
		return fmt.Errorf("candidate step %d exceeds final step", candidate.Step)
	}
	state := machine.InitialState()
	for state.Step < candidate.Step {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		state, err = machine.Step(state)
		if err != nil {
			return fmt.Errorf("independent replay at step %d: %w", state.Step+1, err)
		}
	}
	wantState, err := machine.MarshalState(state)
	if err != nil {
		return fmt.Errorf("marshal independently replayed state: %w", err)
	}
	gotState, err := machine.MarshalState(candidate)
	if err != nil {
		return fmt.Errorf("marshal candidate state: %w", err)
	}
	if !bytes.Equal(wantState, gotState) {
		return fmt.Errorf("candidate state differs from independent replay")
	}
	hash, err := machine.HashState(candidate)
	if err != nil {
		return fmt.Errorf("hash independently verified state: %w", err)
	}
	if hash != wantHash {
		return fmt.Errorf("candidate state hash differs from commitment")
	}
	return nil
}

// QuorumVerifier is the future two-honest-of-three policy. It intentionally
// does not claim collusion resistance: it accepts the largest independently
// identified matching group and requires at least two of three candidates.
// The coordinator currently uses StrictVerifier by default and supplies one
// candidate at a time; wiring three candidates is a separate protocol mode.
type QuorumVerifier struct{}

func (QuorumVerifier) Verify(ctx context.Context, machine *job.Machine, candidates []Candidate) (int, error) {
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	if len(candidates) != 3 {
		return -1, fmt.Errorf("two-honest-of-three verifier requires three candidates")
	}
	counts := make(map[protocol.Digest][]int)
	for index, candidate := range candidates {
		if err := machine.ValidateState(candidate.State); err != nil {
			return -1, err
		}
		hash, err := machine.HashState(candidate.State)
		if err != nil {
			return -1, err
		}
		if hash != candidate.Hash {
			return -1, fmt.Errorf("candidate %q hash does not match state", candidate.WorkerID)
		}
		counts[candidate.Hash] = append(counts[candidate.Hash], index)
	}
	best := []int(nil)
	for _, indexes := range counts {
		if len(indexes) > len(best) {
			best = indexes
		}
	}
	if len(best) < 2 {
		return -1, fmt.Errorf("no two-of-three candidate quorum")
	}
	return best[0], nil
}
