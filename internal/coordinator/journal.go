package coordinator

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/0xciph3r/mini-verde/internal/job"
	"github.com/0xciph3r/mini-verde/internal/protocol"
	"github.com/0xciph3r/mini-verde/internal/wal"
)

type durableJobStart struct {
	Job []byte `json:"job"`
}

type durableJobResult struct {
	Result Result `json:"result"`
	State  []byte `json:"state,omitempty"`
}

func (coordinator *Coordinator) recordTransition(jobID protocol.Digest, key, kind string, payload any) error {
	if coordinator.journal == nil {
		return nil
	}
	jobIDText := protocol.EncodeDigest(jobID)
	transition, err := coordinator.journal.AppendTransition(key, jobIDText, kind, payload)
	if err != nil {
		return err
	}
	_, err = coordinator.journal.AppendAcknowledgement("ack/"+key, transition)
	return err
}

func (coordinator *Coordinator) recordTerminalWithMachine(machine *job.Machine, result Result) error {
	var state []byte
	if result.Status == Accepted {
		var err error
		state, err = machine.MarshalState(result.State)
		if err != nil {
			return err
		}
	}
	return coordinator.recordTransition(
		result.JobID,
		"job/"+protocol.EncodeDigest(result.JobID)+"/terminal",
		"job_terminal",
		durableJobResult{Result: result, State: state},
	)
}

// Recover returns the durably acknowledged terminal result when one exists.
// If the last run was interrupted before its terminal acknowledgement, the
// canonical job is replayed from its durable start record. New attempts are
// fenced by fresh attempt IDs, while the old transcript remains audit data.
func (coordinator *Coordinator) Recover(ctx context.Context, jobID string) (Result, error) {
	if coordinator.journal == nil {
		return Result{}, fmt.Errorf("coordinator has no write-ahead log")
	}
	wantID, err := protocol.DecodeDigest(jobID)
	if err != nil {
		return Result{}, err
	}
	snapshot := coordinator.journal.Replay()
	var start wal.Event
	var terminal wal.Event
	for _, event := range snapshot.Events {
		if event.JobID != jobID {
			continue
		}
		switch event.Kind {
		case "job_started":
			start = event
		case "job_terminal":
			terminal = event
		}
	}
	if terminal.Sequence != 0 {
		var durable durableJobResult
		if err := json.Unmarshal(terminal.Payload, &durable); err != nil {
			return Result{}, fmt.Errorf("decode terminal journal record: %w", err)
		}
		if durable.Result.JobID != wantID {
			return Result{}, fmt.Errorf("terminal journal job ID mismatch")
		}
		if durable.Result.Status == Accepted {
			spec, err := job.UnmarshalSpec(durableStartJob(snapshot, jobID))
			if err != nil {
				return Result{}, err
			}
			machine, err := job.NewMachine(spec)
			if err != nil {
				return Result{}, err
			}
			state, err := machine.UnmarshalState(durable.State)
			if err != nil {
				return Result{}, fmt.Errorf("decode terminal state: %w", err)
			}
			durable.Result.State = state
		}
		if !snapshot.Acknowledged[terminal.Sequence] {
			if _, err := coordinator.journal.AppendAcknowledgement("ack/"+terminal.Key, terminal); err != nil {
				return Result{}, fmt.Errorf("acknowledge recovered terminal result: %w", err)
			}
		}
		return durable.Result, nil
	}
	if start.Sequence == 0 {
		return Result{}, fmt.Errorf("no durable job start for %s", jobID)
	}
	var payload durableJobStart
	if err := json.Unmarshal(start.Payload, &payload); err != nil {
		return Result{}, fmt.Errorf("decode job start journal record: %w", err)
	}
	spec, err := job.UnmarshalSpec(payload.Job)
	if err != nil {
		return Result{}, fmt.Errorf("decode recovered job: %w", err)
	}
	return coordinator.Run(ctx, spec)
}

func durableStartJob(snapshot wal.Snapshot, jobID string) []byte {
	for _, event := range snapshot.Events {
		if event.JobID == jobID && event.Kind == "job_started" {
			var payload durableJobStart
			if json.Unmarshal(event.Payload, &payload) == nil {
				return payload.Job
			}
		}
	}
	return nil
}
