package wal

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestJSONLJournalFsyncsSequencesAndReplaysIdempotently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "coordinator.jsonl")
	log, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	transition, err := log.AppendTransition("job/1/start", "job-1", "job_started", map[string]any{"step": 0})
	if err != nil {
		t.Fatalf("AppendTransition() error = %v", err)
	}
	if transition.Sequence != 1 {
		t.Fatalf("transition sequence = %d, want 1", transition.Sequence)
	}
	ack, err := log.AppendAcknowledgement("ack/job/1/start", transition)
	if err != nil {
		t.Fatalf("AppendAcknowledgement() error = %v", err)
	}
	if ack.Sequence != 2 {
		t.Fatalf("ack sequence = %d, want 2", ack.Sequence)
	}
	if duplicate, err := log.AppendTransition("job/1/start", "job-1", "job_started", map[string]any{"step": 0}); err != nil || duplicate.Sequence != transition.Sequence {
		t.Fatalf("idempotent transition = %+v, error = %v", duplicate, err)
	}
	if duplicate, err := log.AppendAcknowledgement("ack/job/1/start", transition); err != nil || duplicate.Sequence != ack.Sequence {
		t.Fatalf("idempotent acknowledgement = %+v, error = %v", duplicate, err)
	}
	if _, err := log.AppendTransition("job/1/start", "job-1", "job_started", map[string]any{"step": 1}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("conflicting transition error = %v, want ErrDuplicateKey", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer reopened.Close()
	snapshot := reopened.Replay()
	if len(snapshot.Events) != 2 || !snapshot.Acknowledged[transition.Sequence] {
		t.Fatalf("replayed snapshot = %+v", snapshot)
	}
	last, err := reopened.AppendTransition("job/1/end", "job-1", "job_completed", map[string]any{"status": "Accepted"})
	if err != nil {
		t.Fatalf("append after replay error = %v", err)
	}
	if last.Sequence != 3 {
		t.Fatalf("replayed next sequence = %d, want 3", last.Sequence)
	}
}
