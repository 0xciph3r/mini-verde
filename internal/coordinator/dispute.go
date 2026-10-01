package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/0xciph3r/mini-verde/internal/dispute"
	"github.com/0xciph3r/mini-verde/internal/job"
	"github.com/0xciph3r/mini-verde/internal/merkle"
	"github.com/0xciph3r/mini-verde/internal/protocol"
)

type verifiedOpening struct {
	hash protocol.Digest
	bad  bool
	err  error
}

func (coordinator *Coordinator) resolveDispute(
	ctx context.Context,
	machine *job.Machine,
	result Result,
) (Result, error) {
	finalStep := machine.LeafCount() - 1
	result.Dispute = &DisputeSummary{Cost: DisputeCost{
		WorkerRecomputeStepsExpected: map[string]uint64{
			result.Commitments[0].WorkerID: 0,
			result.Commitments[1].WorkerID: 0,
		},
	}}
	maxRound := dispute.CeilLog2(finalStep) + 5
	round := uint64(1)

	// The first query binds each advertised final hash to its own root.
	finalOpenings, err := coordinator.openPair(ctx, machine, result, finalStep, round)
	result.Dispute.Cost.StepHashRounds++
	if err != nil {
		return result, err
	}
	round++
	badFinal := make([]int, 0, 2)
	for i, opening := range finalOpenings {
		if opening.bad || opening.hash != result.Commitments[i].FinalStateHash {
			result.Findings = append(result.Findings, newFinding(
				result.Commitments[i].WorkerID, protocol.VerdictBadOpening, finalStep,
			))
			badFinal = append(badFinal, i)
		}
	}
	if len(badFinal) != 0 {
		return coordinator.finishBoundaryFault(ctx, machine, result, finalOpenings, badFinal, round, maxRound)
	}

	initialState := machine.InitialState()
	initialHash, err := machine.HashState(initialState)
	if err != nil {
		return result, fmt.Errorf("hash initial state: %w", err)
	}
	initialOpenings, err := coordinator.openPair(ctx, machine, result, 0, round)
	result.Dispute.Cost.StepHashRounds++
	if err != nil {
		return result, err
	}
	round++
	badInitial := make([]int, 0, 2)
	for i, opening := range initialOpenings {
		verdict := ""
		if opening.bad {
			verdict = protocol.VerdictBadOpening
		} else if opening.hash != initialHash {
			verdict = protocol.VerdictWrongStep
		}
		if verdict != "" {
			result.Findings = append(result.Findings, newFinding(result.Commitments[i].WorkerID, verdict, 0))
			badInitial = append(badInitial, i)
		}
	}
	if len(badInitial) != 0 {
		return coordinator.finishBoundaryFault(ctx, machine, result, finalOpenings, badInitial, round, maxRound)
	}

	game, err := dispute.New(finalStep, initialHash, [2]dispute.Digest{
		finalOpenings[0].hash, finalOpenings[1].hash,
	})
	if err != nil {
		return result, err
	}
	for {
		index, more, err := game.Next()
		if err != nil {
			return result, fmt.Errorf("advance dispute: %w", err)
		}
		if !more {
			break
		}
		if round > maxRound {
			return result, fmt.Errorf("dispute exceeded round bound %d", maxRound)
		}
		openings, err := coordinator.openPair(ctx, machine, result, index, round)
		result.Dispute.Cost.StepHashRounds++
		result.Dispute.Cost.BisectionRounds++
		if err != nil {
			return result, err
		}
		round++
		bad := make([]int, 0, 2)
		for i, opening := range openings {
			if opening.bad {
				result.Findings = append(result.Findings, newFinding(
					result.Commitments[i].WorkerID, protocol.VerdictBadOpening, index,
				))
				bad = append(bad, i)
			}
		}
		if len(bad) != 0 {
			return coordinator.finishBoundaryFault(ctx, machine, result, finalOpenings, bad, round, maxRound)
		}
		if err := game.Observe(index, [2]dispute.Digest{openings[0].hash, openings[1].hash}); err != nil {
			return result, fmt.Errorf("observe dispute midpoint: %w", err)
		}
	}
	transition, err := game.Transition()
	if err != nil {
		return result, err
	}
	result.Dispute.LowStep = transition.LowStep
	result.Dispute.HighStep = transition.HighStep
	if transition.Rounds != result.Dispute.Cost.BisectionRounds {
		return result, fmt.Errorf("bisection accounting mismatch")
	}

	if round > maxRound {
		return result, fmt.Errorf("dispute exceeded round bound %d", maxRound)
	}
	state, badState, err := coordinator.fetchIndexedState(
		ctx, coordinator.endpoints[0], result.JobID, result.Commitments[0].AttemptID,
		machine, transition.LowStep, transition.AgreedHash, round,
	)
	result.Dispute.Cost.WorkerRecomputeStepsExpected[result.Commitments[0].WorkerID] +=
		transition.LowStep % protocol.CheckpointInterval
	if err != nil && !badState {
		return result, fmt.Errorf("fetch agreed state from %s: %w", result.Commitments[0].WorkerID, err)
	}
	if badState {
		result.Findings = append(result.Findings, newFinding(
			result.Commitments[0].WorkerID, protocol.VerdictBadState, transition.LowStep,
		))
		state, badState, err = coordinator.fetchIndexedState(
			ctx, coordinator.endpoints[1], result.JobID, result.Commitments[1].AttemptID,
			machine, transition.LowStep, transition.AgreedHash, round,
		)
		result.Dispute.Cost.WorkerRecomputeStepsExpected[result.Commitments[1].WorkerID] +=
			transition.LowStep % protocol.CheckpointInterval
		if err != nil && !badState {
			return result, fmt.Errorf("fetch agreed state from %s: %w", result.Commitments[1].WorkerID, err)
		}
		if badState {
			result.Findings = append(result.Findings, newFinding(
				result.Commitments[1].WorkerID, protocol.VerdictBadState, transition.LowStep,
			))
			result.Status = Rejected
			result.Dispute.Cost.LogicalRounds = round + 1
			result.CleanupErrors = coordinator.closeAttempts(ctx, result.JobID, result.Commitments, round+1)
			return result, nil
		}
		return coordinator.acceptProvenWinner(ctx, machine, result, finalOpenings, 1, round+1, maxRound)
	}
	round++

	next, err := machine.Step(state)
	if err != nil {
		return result, fmt.Errorf("referee step: %w", err)
	}
	result.Dispute.Cost.RefereeSteps = 1
	expectedHash, err := machine.HashState(next)
	if err != nil {
		return result, fmt.Errorf("hash referee result: %w", err)
	}
	wrong := make([]int, 0, 2)
	for i, claimedHash := range transition.HighHashes {
		if claimedHash != expectedHash {
			result.Findings = append(result.Findings, newFinding(
				result.Commitments[i].WorkerID, protocol.VerdictWrongStep, transition.HighStep,
			))
			wrong = append(wrong, i)
		}
	}
	switch len(wrong) {
	case 1:
		return coordinator.acceptProvenWinner(ctx, machine, result, finalOpenings, 1-wrong[0], round, maxRound)
	case 2:
		result.Status = Rejected
		result.OverallVerdict = protocol.VerdictBothWrong
		result.Dispute.Cost.LogicalRounds = round
		result.CleanupErrors = coordinator.closeAttempts(ctx, result.JobID, result.Commitments, round)
		return result, nil
	default:
		return result, fmt.Errorf("disagreed high hashes both matched the referee")
	}
}

func (coordinator *Coordinator) openPair(
	ctx context.Context,
	machine *job.Machine,
	result Result,
	index, round uint64,
) ([2]verifiedOpening, error) {
	var openings [2]verifiedOpening
	var group sync.WaitGroup
	group.Add(2)
	for i := range coordinator.endpoints {
		i := i
		go func() {
			defer group.Done()
			openings[i] = coordinator.requestOpening(
				ctx, coordinator.endpoints[i], result.JobID, result.Commitments[i],
				machine.LeafCount(), index, round,
			)
		}()
	}
	group.Wait()
	for i, opening := range openings {
		if opening.err != nil {
			return openings, fmt.Errorf("open step %d on %s: %w", index, result.Commitments[i].WorkerID, opening.err)
		}
	}
	return openings, nil
}

func (coordinator *Coordinator) requestOpening(
	ctx context.Context,
	endpoint Endpoint,
	jobID protocol.Digest,
	commitment Commitment,
	leafCount, index, round uint64,
) verifiedOpening {
	meta := protocol.Meta{JobID: protocol.EncodeDigest(jobID), AttemptID: commitment.AttemptID, Round: round}
	var response protocol.StepHashResponse
	if err := coordinator.post(ctx, endpoint.URL+protocol.StepHashPath, protocol.StepHashRequest{
		Meta: meta, Index: index,
	}, &response); err != nil {
		if errors.Is(err, errInvalidWorkerResponse) {
			return verifiedOpening{bad: true}
		}
		return verifiedOpening{err: err}
	}
	if !protocol.SameMeta(response.Meta, meta) || response.WorkerID != endpoint.ID ||
		response.Proof.Index != index {
		return verifiedOpening{bad: true}
	}
	hash, err := protocol.DecodeDigest(response.Hash)
	if err != nil {
		return verifiedOpening{bad: true}
	}
	proof := merkle.Proof{Index: response.Proof.Index, Siblings: make([]merkle.Digest, len(response.Proof.Siblings))}
	for i, encoded := range response.Proof.Siblings {
		proof.Siblings[i], err = protocol.DecodeDigest(encoded)
		if err != nil {
			return verifiedOpening{bad: true}
		}
	}
	if !merkle.Verify(commitment.Root, jobID, leafCount, hash, proof) {
		return verifiedOpening{bad: true}
	}
	return verifiedOpening{hash: hash}
}

func (coordinator *Coordinator) fetchIndexedState(
	ctx context.Context,
	endpoint Endpoint,
	jobID protocol.Digest,
	attemptID uint64,
	machine *job.Machine,
	index uint64,
	wantHash protocol.Digest,
	round uint64,
) (job.State, bool, error) {
	meta := protocol.Meta{JobID: protocol.EncodeDigest(jobID), AttemptID: attemptID, Round: round}
	var response protocol.StateResponse
	if err := coordinator.post(ctx, endpoint.URL+protocol.StatePath, protocol.StateRequest{
		Meta: meta, Index: index,
	}, &response); err != nil {
		if errors.Is(err, errInvalidWorkerResponse) {
			return job.State{}, true, err
		}
		return job.State{}, false, err
	}
	if !protocol.SameMeta(response.Meta, meta) || response.WorkerID != endpoint.ID || response.Index != index {
		return job.State{}, true, fmt.Errorf("indexed state response metadata does not match request")
	}
	state, err := machine.UnmarshalState(response.State)
	if err != nil {
		return job.State{}, true, err
	}
	if state.Step != index {
		return job.State{}, true, fmt.Errorf("state step %d, want %d", state.Step, index)
	}
	hash, err := machine.HashState(state)
	if err != nil {
		return job.State{}, true, err
	}
	if hash != wantHash {
		return job.State{}, true, fmt.Errorf("indexed state hash does not match agreed hash")
	}
	return state, false, nil
}

func (coordinator *Coordinator) finishBoundaryFault(
	ctx context.Context,
	machine *job.Machine,
	result Result,
	finalOpenings [2]verifiedOpening,
	faulty []int,
	round, maxRound uint64,
) (Result, error) {
	if len(faulty) == 1 {
		return coordinator.acceptProvenWinner(ctx, machine, result, finalOpenings, 1-faulty[0], round, maxRound)
	}
	result.Status = Rejected
	if allFindingsAre(result.Findings, protocol.VerdictWrongStep) {
		result.OverallVerdict = protocol.VerdictBothWrong
	}
	if round > maxRound {
		return result, fmt.Errorf("dispute exceeded round bound %d", maxRound)
	}
	result.Dispute.Cost.LogicalRounds = round
	result.CleanupErrors = coordinator.closeAttempts(ctx, result.JobID, result.Commitments, round)
	return result, nil
}

func (coordinator *Coordinator) acceptProvenWinner(
	ctx context.Context,
	machine *job.Machine,
	result Result,
	finalOpenings [2]verifiedOpening,
	winner int,
	round, maxRound uint64,
) (Result, error) {
	if round+1 > maxRound {
		return result, fmt.Errorf("dispute exceeded round bound %d", maxRound)
	}
	winnerCommitment := result.Commitments[winner]
	state, badState, err := coordinator.fetchAndValidateFinalState(
		ctx, coordinator.endpoints[winner], result.JobID, winnerCommitment.AttemptID,
		machine, finalOpenings[winner].hash, round,
	)
	if err != nil && !badState {
		return result, fmt.Errorf("fetch winner final state from %s: %w", winnerCommitment.WorkerID, err)
	}
	if badState {
		result.Findings = append(result.Findings, newFinding(
			winnerCommitment.WorkerID, protocol.VerdictBadState, machine.LeafCount()-1,
		))
		result.Status = Rejected
		result.Dispute.Cost.LogicalRounds = round + 1
		result.CleanupErrors = coordinator.closeAttempts(ctx, result.JobID, result.Commitments, round+1)
		return result, nil
	}
	if len(result.Findings) != 1 {
		return result, fmt.Errorf("winner selected without exactly one proven faulty worker")
	}
	fault := result.Findings[0]
	result.Status = Accepted
	result.State = state
	result.StateHash = finalOpenings[winner].hash
	result.Acceptance = &AcceptanceBasis{
		Kind: BasisLoserProvenFaulty, FaultyWorker: fault.WorkerID, Verdict: fault.Verdict,
	}
	result.Dispute.WinnerID = winnerCommitment.WorkerID
	result.Dispute.Cost.LogicalRounds = round + 1
	result.CleanupErrors = coordinator.closeAttempts(ctx, result.JobID, result.Commitments, round+1)
	return result, nil
}

func newFinding(workerID, verdict string, step uint64) Finding {
	ownedStep := step
	return Finding{WorkerID: workerID, Verdict: verdict, Step: &ownedStep}
}

func allFindingsAre(findings []Finding, verdict string) bool {
	if len(findings) == 0 {
		return false
	}
	for _, finding := range findings {
		if finding.Verdict != verdict {
			return false
		}
	}
	return true
}
