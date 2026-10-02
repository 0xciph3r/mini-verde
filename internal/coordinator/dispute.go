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
	pair activePair,
	result Result,
	clock *roundClock,
) (Result, error) {
	finalStep := machine.LeafCount() - 1
	result.Dispute = &DisputeSummary{PairGeneration: pair.generation, Cost: DisputeCost{
		WorkerRecomputeStepsExpected: map[string]uint64{
			result.Commitments[0].WorkerID: 0,
			result.Commitments[1].WorkerID: 0,
		},
	}}
	maxLogicalRounds := dispute.CeilLog2(finalStep) + 5
	logicalRounds := uint64(0)

	// The first query binds each advertised final hash to its own root.
	round, err := nextDisputeRound(result.Dispute, &logicalRounds, maxLogicalRounds, clock)
	if err != nil {
		return result, err
	}
	finalOpenings, err := coordinator.openPair(ctx, machine, pair, result, finalStep, round)
	result.Dispute.Cost.StepHashRounds++
	if err != nil {
		return result, err
	}
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
		return coordinator.finishBoundaryFault(
			ctx, machine, pair, result, finalOpenings, badFinal,
			&logicalRounds, maxLogicalRounds, clock,
		)
	}

	initialState := machine.InitialState()
	initialHash, err := machine.HashState(initialState)
	if err != nil {
		return result, fmt.Errorf("hash initial state: %w", err)
	}
	round, err = nextDisputeRound(result.Dispute, &logicalRounds, maxLogicalRounds, clock)
	if err != nil {
		return result, err
	}
	initialOpenings, err := coordinator.openPair(ctx, machine, pair, result, 0, round)
	result.Dispute.Cost.StepHashRounds++
	if err != nil {
		return result, err
	}
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
		return coordinator.finishBoundaryFault(
			ctx, machine, pair, result, finalOpenings, badInitial,
			&logicalRounds, maxLogicalRounds, clock,
		)
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
		round, err = nextDisputeRound(result.Dispute, &logicalRounds, maxLogicalRounds, clock)
		if err != nil {
			return result, err
		}
		openings, err := coordinator.openPair(ctx, machine, pair, result, index, round)
		result.Dispute.Cost.StepHashRounds++
		result.Dispute.Cost.BisectionRounds++
		if err != nil {
			return result, err
		}
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
			return coordinator.finishBoundaryFault(
				ctx, machine, pair, result, finalOpenings, bad,
				&logicalRounds, maxLogicalRounds, clock,
			)
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

	round, err = nextDisputeRound(result.Dispute, &logicalRounds, maxLogicalRounds, clock)
	if err != nil {
		return result, err
	}
	state, badState, err := coordinator.fetchIndexedState(
		ctx, pair.replicas[0].endpoint, result.JobID, result.Commitments[0].AttemptID,
		machine, transition.LowStep, transition.AgreedHash, round,
	)
	result.Dispute.Cost.WorkerRecomputeStepsExpected[result.Commitments[0].WorkerID] +=
		transition.LowStep % protocol.CheckpointInterval
	firstUnavailable := err != nil && !badState && errors.Is(err, errWorkerUnavailable)
	if err != nil && !badState && !firstUnavailable {
		return result, fmt.Errorf("fetch agreed state from %s: %w", result.Commitments[0].WorkerID, err)
	}
	if firstUnavailable {
		result.Findings = append(result.Findings, timeoutFinding(result.Commitments[0].WorkerID, "agreed-state"))
	}
	if badState || firstUnavailable {
		firstBadState := badState
		if badState {
			result.Findings = append(result.Findings, newFinding(
				result.Commitments[0].WorkerID, protocol.VerdictBadState, transition.LowStep,
			))
		}
		state, badState, err = coordinator.fetchIndexedState(
			ctx, pair.replicas[1].endpoint, result.JobID, result.Commitments[1].AttemptID,
			machine, transition.LowStep, transition.AgreedHash, round,
		)
		result.Dispute.Cost.WorkerRecomputeStepsExpected[result.Commitments[1].WorkerID] +=
			transition.LowStep % protocol.CheckpointInterval
		if err != nil && !badState && !errors.Is(err, errWorkerUnavailable) {
			return result, fmt.Errorf("fetch agreed state from %s: %w", result.Commitments[1].WorkerID, err)
		}
		if err != nil && !badState {
			result.Findings = append(result.Findings, timeoutFinding(result.Commitments[1].WorkerID, "agreed-state"))
			slots := []int{1}
			if firstUnavailable {
				slots = []int{0, 1}
			}
			return result, &unavailableError{slots: slots, phase: "agreed-state", err: err}
		}
		if badState {
			result.Findings = append(result.Findings, newFinding(
				result.Commitments[1].WorkerID, protocol.VerdictBadState, transition.LowStep,
			))
			if firstUnavailable {
				return result, &unavailableError{
					slots: []int{0}, phase: "agreed-state", err: errors.New("peer returned a bad agreed state"),
				}
			}
			result.Status = Rejected
			closeRound, roundErr := nextDisputeRound(result.Dispute, &logicalRounds, maxLogicalRounds, clock)
			if roundErr != nil {
				return result, roundErr
			}
			result.CleanupErrors = coordinator.closePair(ctx, result.JobID, pair, closeRound)
			return result, nil
		}
		if firstBadState {
			return coordinator.acceptProvenWinner(
				ctx, machine, pair, result, finalOpenings, 1,
				&logicalRounds, maxLogicalRounds, clock,
			)
		}
	}

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
		return coordinator.acceptProvenWinner(
			ctx, machine, pair, result, finalOpenings, 1-wrong[0],
			&logicalRounds, maxLogicalRounds, clock,
		)
	case 2:
		result.Status = Rejected
		result.OverallVerdict = protocol.VerdictBothWrong
		closeRound, roundErr := nextDisputeRound(result.Dispute, &logicalRounds, maxLogicalRounds, clock)
		if roundErr != nil {
			return result, roundErr
		}
		result.CleanupErrors = coordinator.closePair(ctx, result.JobID, pair, closeRound)
		return result, nil
	default:
		return result, fmt.Errorf("disagreed high hashes both matched the referee")
	}
}

func (coordinator *Coordinator) openPair(
	ctx context.Context,
	machine *job.Machine,
	pair activePair,
	result Result,
	index, round uint64,
) ([2]verifiedOpening, error) {
	var openings [2]verifiedOpening
	var group sync.WaitGroup
	group.Add(2)
	for i := range pair.replicas {
		i := i
		go func() {
			defer group.Done()
			openings[i] = coordinator.requestOpening(
				ctx, pair.replicas[i].endpoint, result.JobID, result.Commitments[i],
				machine.LeafCount(), index, round,
			)
		}()
	}
	group.Wait()
	var unavailableSlots []int
	var unavailableErrors []error
	for i, opening := range openings {
		if opening.err != nil {
			if errors.Is(opening.err, errWorkerUnavailable) {
				unavailableSlots = append(unavailableSlots, i)
				unavailableErrors = append(unavailableErrors, opening.err)
				continue
			}
			return openings, fmt.Errorf(
				"open step %d on %s: %w", index, result.Commitments[i].WorkerID, opening.err,
			)
		}
	}
	if len(unavailableSlots) != 0 {
		return openings, &unavailableError{
			slots: unavailableSlots,
			phase: "step-hash",
			err:   errors.Join(unavailableErrors...),
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
	pair activePair,
	result Result,
	finalOpenings [2]verifiedOpening,
	faulty []int,
	logicalRounds *uint64,
	maxLogicalRounds uint64,
	clock *roundClock,
) (Result, error) {
	if len(faulty) == 1 {
		return coordinator.acceptProvenWinner(
			ctx, machine, pair, result, finalOpenings, 1-faulty[0],
			logicalRounds, maxLogicalRounds, clock,
		)
	}
	result.Status = Rejected
	if allObjectiveFindingsAre(result.Findings, protocol.VerdictWrongStep) {
		result.OverallVerdict = protocol.VerdictBothWrong
	}
	closeRound, err := nextDisputeRound(result.Dispute, logicalRounds, maxLogicalRounds, clock)
	if err != nil {
		return result, err
	}
	result.CleanupErrors = coordinator.closePair(ctx, result.JobID, pair, closeRound)
	return result, nil
}

func (coordinator *Coordinator) acceptProvenWinner(
	ctx context.Context,
	machine *job.Machine,
	pair activePair,
	result Result,
	finalOpenings [2]verifiedOpening,
	winner int,
	logicalRounds *uint64,
	maxLogicalRounds uint64,
	clock *roundClock,
) (Result, error) {
	round, err := nextDisputeRound(result.Dispute, logicalRounds, maxLogicalRounds, clock)
	if err != nil {
		return result, err
	}
	winnerCommitment := result.Commitments[winner]
	state, badState, err := coordinator.fetchAndValidateFinalState(
		ctx, pair.replicas[winner].endpoint, result.JobID, winnerCommitment.AttemptID,
		machine, finalOpenings[winner].hash, round,
	)
	if err != nil && !badState {
		if errors.Is(err, errWorkerUnavailable) {
			result.Findings = append(result.Findings, timeoutFinding(winnerCommitment.WorkerID, "winner-final-state"))
			return result, &unavailableError{slots: []int{winner}, phase: "winner-final-state", err: err}
		}
		return result, fmt.Errorf("fetch winner final state from %s: %w", winnerCommitment.WorkerID, err)
	}
	if badState {
		result.Findings = append(result.Findings, newFinding(
			winnerCommitment.WorkerID, protocol.VerdictBadState, machine.LeafCount()-1,
		))
		result.Status = Rejected
		closeRound, roundErr := nextDisputeRound(result.Dispute, logicalRounds, maxLogicalRounds, clock)
		if roundErr != nil {
			return result, roundErr
		}
		result.CleanupErrors = coordinator.closePair(ctx, result.JobID, pair, closeRound)
		return result, nil
	}
	objective := objectiveFindings(result.Findings)
	if len(objective) != 1 {
		return result, fmt.Errorf("winner selected without exactly one proven faulty worker")
	}
	fault := objective[0]
	result.Status = Accepted
	result.State = state
	result.StateHash = finalOpenings[winner].hash
	result.Acceptance = &AcceptanceBasis{
		Kind: BasisLoserProvenFaulty, FaultyWorker: fault.WorkerID, Verdict: fault.Verdict,
	}
	result.Dispute.WinnerID = winnerCommitment.WorkerID
	closeRound, roundErr := nextDisputeRound(result.Dispute, logicalRounds, maxLogicalRounds, clock)
	if roundErr != nil {
		return result, roundErr
	}
	result.CleanupErrors = coordinator.closePair(ctx, result.JobID, pair, closeRound)
	return result, nil
}

func newFinding(workerID, verdict string, step uint64) Finding {
	ownedStep := step
	return Finding{WorkerID: workerID, Verdict: verdict, Step: &ownedStep}
}

func allObjectiveFindingsAre(findings []Finding, verdict string) bool {
	objective := objectiveFindings(findings)
	if len(objective) == 0 {
		return false
	}
	for _, finding := range objective {
		if finding.Verdict != verdict {
			return false
		}
	}
	return true
}

func nextDisputeRound(
	summary *DisputeSummary,
	logicalRounds *uint64,
	maxLogicalRounds uint64,
	clock *roundClock,
) (uint64, error) {
	*logicalRounds++
	summary.Cost.LogicalRounds = *logicalRounds
	if *logicalRounds > maxLogicalRounds {
		return 0, fmt.Errorf("dispute exceeded round bound %d", maxLogicalRounds)
	}
	return clock.take(), nil
}
