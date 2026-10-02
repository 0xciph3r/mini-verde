package coordinator

import (
	"sync"

	"github.com/0xciph3r/mini-verde/internal/protocol"
)

// Reputation separates successful participation, objective computational
// faults, and availability suspicions. Timeouts never count as losses.
type Reputation struct {
	Wins     uint64 `json:"wins"`
	Losses   uint64 `json:"losses"`
	Timeouts uint64 `json:"timeouts"`
}

type ReputationEvent struct {
	Sequence uint64 `json:"sequence"`
	WorkerID string `json:"worker_id"`
	Kind     string `json:"kind"`
	Verdict  string `json:"verdict,omitempty"`
}

type reputationLog struct {
	mu      sync.Mutex
	values  map[string]Reputation
	events  []ReputationEvent
	nextSeq uint64
}

func (log *reputationLog) record(result Result) {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.values == nil {
		log.values = make(map[string]Reputation)
	}

	timedOut := make(map[string]struct{})
	lost := make(map[string]struct{})
	for _, finding := range result.Findings {
		switch finding.Verdict {
		case protocol.VerdictTimeout:
			if _, exists := timedOut[finding.WorkerID]; exists {
				continue
			}
			timedOut[finding.WorkerID] = struct{}{}
			value := log.values[finding.WorkerID]
			value.Timeouts++
			log.values[finding.WorkerID] = value
			log.appendLocked(finding.WorkerID, "timeout", protocol.VerdictTimeout)
		case protocol.VerdictWrongStep, protocol.VerdictBadOpening, protocol.VerdictBadState:
			if _, exists := lost[finding.WorkerID]; exists {
				continue
			}
			lost[finding.WorkerID] = struct{}{}
			value := log.values[finding.WorkerID]
			value.Losses++
			log.values[finding.WorkerID] = value
			log.appendLocked(finding.WorkerID, "loss", finding.Verdict)
		}
	}
	if result.Status != Accepted || result.Acceptance == nil {
		return
	}
	winners := make(map[string]struct{})
	switch result.Acceptance.Kind {
	case BasisFinalStateAgreement:
		for _, commitment := range result.Commitments {
			if _, faulty := lost[commitment.WorkerID]; !faulty {
				log.recordWinLocked(commitment.WorkerID, winners)
			}
		}
	case BasisLoserProvenFaulty:
		for _, commitment := range result.Commitments {
			if commitment.WorkerID != result.Acceptance.FaultyWorker {
				log.recordWinLocked(commitment.WorkerID, winners)
			}
		}
	}
}

func (log *reputationLog) recordWinLocked(workerID string, seen map[string]struct{}) {
	if _, exists := seen[workerID]; exists {
		return
	}
	seen[workerID] = struct{}{}
	value := log.values[workerID]
	value.Wins++
	log.values[workerID] = value
	log.appendLocked(workerID, "win", "")
}

func (log *reputationLog) appendLocked(workerID, kind, verdict string) {
	log.nextSeq++
	log.events = append(log.events, ReputationEvent{
		Sequence: log.nextSeq,
		WorkerID: workerID,
		Kind:     kind,
		Verdict:  verdict,
	})
}

func (log *reputationLog) snapshot() map[string]Reputation {
	log.mu.Lock()
	defer log.mu.Unlock()
	values := make(map[string]Reputation, len(log.values))
	for workerID, value := range log.values {
		values[workerID] = value
	}
	return values
}

// ReputationSnapshot returns a copy of the process-local reputation state.
func (coordinator *Coordinator) ReputationSnapshot() map[string]Reputation {
	return coordinator.reputation.snapshot()
}

// ReputationEvents returns a copy of the append-only process-local event log.
func (coordinator *Coordinator) ReputationEvents() []ReputationEvent {
	coordinator.reputation.mu.Lock()
	defer coordinator.reputation.mu.Unlock()
	return append([]ReputationEvent(nil), coordinator.reputation.events...)
}
