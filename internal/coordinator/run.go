package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/0xciph3r/mini-verde/internal/job"
	"github.com/0xciph3r/mini-verde/internal/protocol"
)

type jobRun struct {
	coordinator *Coordinator
	ctx         context.Context
	machine     *job.Machine
	encodedJob  []byte
	jobID       protocol.Digest
	clock       roundClock
	nextWorker  int
	attempts    []AttemptSummary
	timeouts    []Finding
	evidence    []Finding
	disputes    []DisputeSummary
	timedOut    map[string]struct{}
	generations uint64
	provenFault *Finding
	journalErr  error
}

func (run *jobRun) execute() (Result, error) {
	var pair activePair
	for {
		if cause := context.Cause(run.ctx); cause != nil {
			return run.finishUnresolved(pair), run.contextError(cause)
		}
		if run.journalErr != nil {
			return run.finishUnresolved(pair), run.journalErr
		}
		if !run.fillPair(&pair) {
			return run.finishUnresolved(pair), nil
		}
		if run.journalErr != nil {
			return run.finishUnresolved(pair), run.journalErr
		}

		failed, err := run.executePending(&pair)
		if err != nil {
			if cause := context.Cause(run.ctx); cause != nil {
				return run.finishUnresolved(pair), run.contextError(cause)
			}
			return run.partialResult(pair), err
		}
		if run.journalErr != nil {
			return run.finishUnresolved(pair), run.journalErr
		}
		if len(failed) != 0 {
			for _, slot := range failed {
				run.recordTimeout(pair.replicas[slot], "execute")
				run.abandon(pair.replicas[slot])
				pair.replicas[slot] = replica{}
			}
			continue
		}

		run.generations++
		pair.generation = run.generations
		for i := range pair.replicas {
			record := pair.replicas[i].record
			if run.attempts[record].PairGeneration == 0 {
				run.attempts[record].PairGeneration = pair.generation
			}
		}
		var result Result
		var pairErr error
		if run.provenFault != nil {
			result, pairErr = run.coordinator.verifyReplacementAfterFault(
				run.ctx, run.machine, pair, *run.provenFault, &run.clock,
			)
		} else {
			result, pairErr = run.coordinator.evaluatePair(run.ctx, run.machine, pair, &run.clock)
		}
		for _, finding := range result.Findings {
			if finding.Verdict == protocol.VerdictTimeout {
				run.recordTimeoutByID(pair, finding.WorkerID, finding.Phase)
			} else if isObjectiveVerdict(finding.Verdict) {
				run.recordEvidence(pair, finding)
			}
		}
		if result.Dispute != nil {
			run.disputes = append(run.disputes, cloneDispute(*result.Dispute))
		}
		if pairErr == nil {
			return run.finish(result, pair), nil
		}
		if run.journalErr != nil {
			return run.finishUnresolved(pair), run.journalErr
		}

		var unavailable *unavailableError
		if errors.As(pairErr, &unavailable) {
			if run.provenFault == nil {
				run.rememberProvenFault(pair, result, unavailable.slots)
			}
			for _, slot := range unavailable.slots {
				run.recordTimeout(pair.replicas[slot], unavailable.phase)
				run.abandon(pair.replicas[slot])
				pair.replicas[slot] = replica{}
			}
			continue
		}
		if cause := context.Cause(run.ctx); cause != nil {
			return run.finishUnresolved(pair), run.contextError(cause)
		}
		return run.partialResult(pair), pairErr
	}
}

func (run *jobRun) rememberProvenFault(pair activePair, result Result, unavailableSlots []int) {
	objective := objectiveFindings(result.Findings)
	if len(objective) != 1 {
		return
	}
	unavailableWorkers := make(map[string]struct{}, len(unavailableSlots))
	for _, slot := range unavailableSlots {
		unavailableWorkers[pair.replicas[slot].endpoint.ID] = struct{}{}
	}
	if _, unavailable := unavailableWorkers[objective[0].WorkerID]; unavailable {
		return
	}
	fault := objective[0]
	run.provenFault = &fault
}

func (run *jobRun) fillPair(pair *activePair) bool {
	missing := 0
	for _, replica := range pair.replicas {
		if replica.endpoint.ID == "" {
			missing++
		}
	}
	if len(run.coordinator.endpoints)-run.nextWorker < missing {
		return false
	}
	for slot := range pair.replicas {
		if pair.replicas[slot].endpoint.ID != "" {
			continue
		}
		endpoint := run.coordinator.endpoints[run.nextWorker]
		run.nextWorker++
		attemptID := run.coordinator.attempts.Add(1)
		record := len(run.attempts)
		run.attempts = append(run.attempts, AttemptSummary{
			WorkerID:  endpoint.ID,
			AttemptID: attemptID,
		})
		run.record("attempt/"+protocol.EncodeDigest(run.jobID)+fmt.Sprintf("/%d", attemptID), "attempt_assigned", map[string]any{
			"attempt_id": attemptID, "worker_id": endpoint.ID,
		})
		pair.replicas[slot] = replica{
			endpoint:   endpoint,
			commitment: Commitment{WorkerID: endpoint.ID, AttemptID: attemptID},
			record:     record,
		}
	}
	return true
}

func (run *jobRun) executePending(pair *activePair) ([]int, error) {
	type execution struct {
		commitment Commitment
		err        error
	}
	var executions [2]execution
	var group sync.WaitGroup
	for slot := range pair.replicas {
		replica := pair.replicas[slot]
		if replica.commitment.Root != (protocol.Digest{}) {
			continue
		}
		group.Add(1)
		go func() {
			defer group.Done()
			executions[slot].commitment, executions[slot].err = run.coordinator.execute(
				run.ctx, replica.endpoint, run.jobID, replica.commitment.AttemptID, run.encodedJob,
			)
		}()
	}
	group.Wait()

	var unavailable []int
	for slot, execution := range executions {
		replica := &pair.replicas[slot]
		if replica.commitment.Root != (protocol.Digest{}) {
			continue
		}
		if execution.err == nil {
			replica.commitment = execution.commitment
			record := &run.attempts[replica.record]
			record.Outcome = AttemptReady
			record.Root = protocol.EncodeDigest(execution.commitment.Root)
			record.FinalStateHash = protocol.EncodeDigest(execution.commitment.FinalStateHash)
			run.record("attempt/"+protocol.EncodeDigest(run.jobID)+fmt.Sprintf("/%d/commitment", replica.commitment.AttemptID), "commitment_ready", execution.commitment)
			continue
		}
		if errors.Is(execution.err, errWorkerUnavailable) {
			unavailable = append(unavailable, slot)
			continue
		}
		return nil, fmt.Errorf("execute on %s: %w", replica.endpoint.ID, execution.err)
	}
	return unavailable, nil
}

func (run *jobRun) recordTimeout(replica replica, phase string) {
	if replica.endpoint.ID == "" {
		return
	}
	if run.timedOut == nil {
		run.timedOut = make(map[string]struct{})
	}
	if _, exists := run.timedOut[replica.endpoint.ID]; exists {
		return
	}
	run.timedOut[replica.endpoint.ID] = struct{}{}
	run.timeouts = append(run.timeouts, Finding{
		WorkerID: replica.endpoint.ID,
		Verdict:  protocol.VerdictTimeout,
		Phase:    phase,
	})
	run.attempts[replica.record].Outcome = AttemptTimedOut
	run.record("attempt/"+protocol.EncodeDigest(run.jobID)+fmt.Sprintf("/%d/timeout/%s", replica.commitment.AttemptID, phase), "attempt_timeout", map[string]string{
		"worker_id": replica.endpoint.ID, "phase": phase,
	})
}

func (run *jobRun) record(key, kind string, payload any) {
	if run.journalErr != nil || run.coordinator.journal == nil {
		return
	}
	if err := run.coordinator.recordTransition(run.jobID, key, kind, payload); err != nil {
		run.journalErr = fmt.Errorf("durably record %s: %w", kind, err)
	}
}

func (run *jobRun) recordTimeoutByID(pair activePair, workerID, phase string) {
	for _, replica := range pair.replicas {
		if replica.endpoint.ID == workerID {
			run.recordTimeout(replica, phase)
			return
		}
	}
}

func (run *jobRun) recordEvidence(pair activePair, finding Finding) {
	run.evidence = mergeFindings(run.evidence, []Finding{finding})
	for _, replica := range pair.replicas {
		if replica.endpoint.ID == finding.WorkerID && run.attempts[replica.record].Outcome != AttemptTimedOut {
			run.attempts[replica.record].Outcome = AttemptFaulty
			return
		}
	}
}

func (run *jobRun) abandon(replica replica) {
	if replica.endpoint.ID == "" {
		return
	}
	round := run.clock.take()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), run.coordinator.requestTimeout)
		defer cancel()
		// An unreachable worker cannot acknowledge cleanup. The retained attempt
		// remains fenced by its attempt ID and is already reported through
		// Timeout. Cleanup must not extend the overall job deadline.
		_ = run.coordinator.closeReplica(ctx, run.jobID, replica, round)
	}()
}

func (run *jobRun) partialResult(pair activePair) Result {
	result := Result{
		JobID:           run.jobID,
		Findings:        mergeFindings(run.timeouts, run.evidence),
		Attempts:        append([]AttemptSummary(nil), run.attempts...),
		PairGenerations: run.generations,
		Replacements:    replacementCount(run.attempts),
		PairDisputes:    append([]DisputeSummary(nil), run.disputes...),
	}
	for _, replica := range pair.replicas {
		if replica.commitment.Root != (protocol.Digest{}) {
			result.Commitments = append(result.Commitments, replica.commitment)
		}
	}
	return result
}

func (run *jobRun) finishUnresolved(pair activePair) Result {
	result := run.partialResult(pair)
	result.Status = Unresolved
	for _, replica := range pair.replicas {
		if replica.endpoint.ID != "" {
			if outcome := run.attempts[replica.record].Outcome; outcome != AttemptTimedOut && outcome != AttemptFaulty {
				run.attempts[replica.record].Outcome = AttemptUnresolved
			}
			run.abandon(replica)
		}
	}
	result.Attempts = append([]AttemptSummary(nil), run.attempts...)
	run.coordinator.reputation.record(result)
	result.Reputation = run.coordinator.reputation.snapshot()
	return result
}

func (run *jobRun) finish(result Result, pair activePair) Result {
	objectives := make(map[string]struct{})
	for _, finding := range result.Findings {
		if isObjectiveVerdict(finding.Verdict) {
			objectives[finding.WorkerID] = struct{}{}
		}
	}
	for _, replica := range pair.replicas {
		record := &run.attempts[replica.record]
		if _, timedOut := run.timedOut[replica.endpoint.ID]; timedOut || record.Outcome == AttemptFaulty {
			continue
		}
		if _, faulty := objectives[replica.endpoint.ID]; faulty {
			record.Outcome = AttemptFaulty
		} else if result.Status == Accepted {
			record.Outcome = AttemptAccepted
		}
	}
	result.Findings = mergeFindings(mergeFindings(run.timeouts, run.evidence), result.Findings)
	result.Attempts = append([]AttemptSummary(nil), run.attempts...)
	result.PairGenerations = run.generations
	result.Replacements = replacementCount(run.attempts)
	result.PairDisputes = append([]DisputeSummary(nil), run.disputes...)
	run.coordinator.reputation.record(result)
	result.Reputation = run.coordinator.reputation.snapshot()
	return result
}

func cloneDispute(summary DisputeSummary) DisputeSummary {
	cloned := summary
	cloned.Cost.WorkerRecomputeStepsExpected = make(
		map[string]uint64, len(summary.Cost.WorkerRecomputeStepsExpected),
	)
	for workerID, steps := range summary.Cost.WorkerRecomputeStepsExpected {
		cloned.Cost.WorkerRecomputeStepsExpected[workerID] = steps
	}
	return cloned
}

func (run *jobRun) contextError(cause error) error {
	if errors.Is(cause, errJobDeadline) {
		return nil
	}
	return cause
}

func replacementCount(attempts []AttemptSummary) uint64 {
	if len(attempts) <= 2 {
		return 0
	}
	return uint64(len(attempts) - 2)
}

func mergeFindings(left, right []Finding) []Finding {
	merged := append([]Finding(nil), left...)
	for _, candidate := range right {
		duplicate := false
		for _, current := range merged {
			if current.WorkerID == candidate.WorkerID && current.Verdict == candidate.Verdict &&
				current.Phase == candidate.Phase && sameStep(current.Step, candidate.Step) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			merged = append(merged, candidate)
		}
	}
	return merged
}

func sameStep(left, right *uint64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func isObjectiveVerdict(verdict string) bool {
	switch verdict {
	case protocol.VerdictWrongStep, protocol.VerdictBadOpening, protocol.VerdictBadState:
		return true
	default:
		return false
	}
}

func objectiveFindings(findings []Finding) []Finding {
	var objective []Finding
	for _, finding := range findings {
		if isObjectiveVerdict(finding.Verdict) {
			objective = append(objective, finding)
		}
	}
	return objective
}

func (coordinator *Coordinator) evaluatePair(
	ctx context.Context,
	machine *job.Machine,
	pair activePair,
	clock *roundClock,
) (Result, error) {
	result := Result{JobID: machine.ID(), Commitments: pair.commitments()}
	if result.Commitments[0].FinalStateHash != result.Commitments[1].FinalStateHash {
		return coordinator.resolveDispute(ctx, machine, pair, result, clock)
	}

	agreedHash := result.Commitments[0].FinalStateHash
	round := clock.take()
	var unavailableSlots []int
	var unavailableErrors []error
	for slot, replica := range pair.replicas {
		state, badState, err := coordinator.fetchAndValidateFinalState(
			ctx, replica.endpoint, result.JobID, replica.commitment.AttemptID, machine, agreedHash, round,
		)
		if err == nil {
			objective := objectiveFindings(result.Findings)
			if len(objective) != 0 {
				openingRound := clock.take()
				opening := coordinator.requestOpening(
					ctx, replica.endpoint, result.JobID, replica.commitment,
					machine.LeafCount(), machine.LeafCount()-1, openingRound,
				)
				if opening.err != nil {
					if errors.Is(opening.err, errWorkerUnavailable) {
						return result, &unavailableError{slots: []int{slot}, phase: "final-opening", err: opening.err}
					}
					return result, fmt.Errorf("open fallback final state on %s: %w", replica.endpoint.ID, opening.err)
				}
				if opening.bad || opening.hash != agreedHash {
					result.Findings = append(result.Findings, newFinding(
						replica.endpoint.ID, protocol.VerdictBadOpening, machine.LeafCount()-1,
					))
					result.Status = Rejected
					result.CleanupErrors = coordinator.closePair(ctx, result.JobID, pair, clock.take())
					return result, nil
				}
			}
			result.Status = Accepted
			result.State = state
			result.StateHash = agreedHash
			if len(objective) == 0 {
				result.Acceptance = &AcceptanceBasis{Kind: BasisFinalStateAgreement}
			} else {
				fault := objective[0]
				result.Acceptance = &AcceptanceBasis{
					Kind: BasisLoserProvenFaulty, FaultyWorker: fault.WorkerID, Verdict: fault.Verdict,
				}
			}
			result.CleanupErrors = coordinator.closePair(ctx, result.JobID, pair, clock.take())
			return result, nil
		}
		if badState {
			result.Findings = append(result.Findings, newFinding(
				replica.endpoint.ID, protocol.VerdictBadState, machine.LeafCount()-1,
			))
			continue
		}
		if errors.Is(err, errWorkerUnavailable) {
			result.Findings = append(result.Findings, timeoutFinding(replica.endpoint.ID, "final-state"))
			unavailableSlots = append(unavailableSlots, slot)
			unavailableErrors = append(unavailableErrors, err)
			continue
		}
		return result, fmt.Errorf("fetch final state from %s: %w", replica.endpoint.ID, err)
	}
	if len(unavailableSlots) != 0 {
		return result, &unavailableError{
			slots: unavailableSlots,
			phase: "final-state",
			err:   errors.Join(unavailableErrors...),
		}
	}
	result.Status = Rejected
	result.CleanupErrors = coordinator.closePair(ctx, result.JobID, pair, clock.take())
	return result, nil
}

func (coordinator *Coordinator) verifyReplacementAfterFault(
	ctx context.Context,
	machine *job.Machine,
	pair activePair,
	fault Finding,
	clock *roundClock,
) (Result, error) {
	result := Result{
		JobID:       machine.ID(),
		Commitments: pair.commitments(),
		Findings:    []Finding{fault},
		Dispute: &DisputeSummary{
			PairGeneration: pair.generation,
			Cost: DisputeCost{WorkerRecomputeStepsExpected: map[string]uint64{
				pair.replicas[0].endpoint.ID: 0,
				pair.replicas[1].endpoint.ID: 0,
			}},
		},
	}
	faultySlot := -1
	for slot, replica := range pair.replicas {
		if replica.endpoint.ID == fault.WorkerID {
			faultySlot = slot
			break
		}
	}
	if faultySlot == -1 {
		return result, fmt.Errorf("proven-faulty worker %q is not in the active pair", fault.WorkerID)
	}
	winner := 1 - faultySlot
	winnerReplica := pair.replicas[winner]
	finalStep := machine.LeafCount() - 1

	openingRound := clock.take()
	result.Dispute.Cost.LogicalRounds++
	result.Dispute.Cost.StepHashRounds++
	opening := coordinator.requestOpening(
		ctx, winnerReplica.endpoint, result.JobID, winnerReplica.commitment,
		machine.LeafCount(), finalStep, openingRound,
	)
	if opening.err != nil {
		if errors.Is(opening.err, errWorkerUnavailable) {
			result.Findings = append(result.Findings, timeoutFinding(winnerReplica.endpoint.ID, "replacement-final-opening"))
			return result, &unavailableError{
				slots: []int{winner}, phase: "replacement-final-opening", err: opening.err,
			}
		}
		return result, fmt.Errorf("open replacement final state on %s: %w", winnerReplica.endpoint.ID, opening.err)
	}
	if opening.bad || opening.hash != winnerReplica.commitment.FinalStateHash {
		result.Findings = append(result.Findings, newFinding(
			winnerReplica.endpoint.ID, protocol.VerdictBadOpening, finalStep,
		))
		result.Status = Rejected
		result.Dispute.Cost.LogicalRounds++
		result.CleanupErrors = coordinator.closePair(ctx, result.JobID, pair, clock.take())
		return result, nil
	}

	stateRound := clock.take()
	result.Dispute.Cost.LogicalRounds++
	state, badState, err := coordinator.fetchAndValidateFinalState(
		ctx, winnerReplica.endpoint, result.JobID, winnerReplica.commitment.AttemptID,
		machine, opening.hash, stateRound,
	)
	if err != nil && !badState {
		if errors.Is(err, errWorkerUnavailable) {
			result.Findings = append(result.Findings, timeoutFinding(winnerReplica.endpoint.ID, "replacement-final-state"))
			return result, &unavailableError{
				slots: []int{winner}, phase: "replacement-final-state", err: err,
			}
		}
		return result, fmt.Errorf("fetch replacement final state from %s: %w", winnerReplica.endpoint.ID, err)
	}
	if badState {
		result.Findings = append(result.Findings, newFinding(
			winnerReplica.endpoint.ID, protocol.VerdictBadState, finalStep,
		))
		result.Status = Rejected
		result.Dispute.Cost.LogicalRounds++
		result.CleanupErrors = coordinator.closePair(ctx, result.JobID, pair, clock.take())
		return result, nil
	}

	result.Status = Accepted
	result.State = state
	result.StateHash = opening.hash
	result.Acceptance = &AcceptanceBasis{
		Kind: BasisLoserProvenFaulty, FaultyWorker: fault.WorkerID, Verdict: fault.Verdict,
	}
	result.Dispute.WinnerID = winnerReplica.endpoint.ID
	result.Dispute.Cost.LogicalRounds++
	result.CleanupErrors = coordinator.closePair(ctx, result.JobID, pair, clock.take())
	return result, nil
}

func timeoutFinding(workerID, phase string) Finding {
	return Finding{WorkerID: workerID, Verdict: protocol.VerdictTimeout, Phase: phase}
}
