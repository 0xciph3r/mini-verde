// Package wal provides the small durable journal used by the coordinator.
// Records are append-only JSON lines so a failed run remains inspectable.
package wal

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

var (
	ErrClosed       = errors.New("write-ahead log is closed")
	ErrDuplicateKey = errors.New("write-ahead log key has different content")
)

// Event is one immutable journal record. Sequence is assigned by Log and is
// strictly increasing across restarts. An acknowledgement is a separate event
// so replay can distinguish a transition that was durable from one whose
// caller acknowledgement was also durably recorded.
type Event struct {
	Sequence uint64          `json:"sequence"`
	Key      string          `json:"key"`
	JobID    string          `json:"job_id,omitempty"`
	Kind     string          `json:"kind"`
	Ref      uint64          `json:"ref,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
}

// Snapshot is the replayed journal state. A transition is durable as soon as
// it appears in Events. Acknowledged contains transition sequences for which
// a separate durable acknowledgement record was replayed.
type Snapshot struct {
	Events       []Event
	Acknowledged map[uint64]bool
}

// Log serializes append and fsync operations. It is safe for concurrent
// coordinator request paths.
type Log struct {
	mu      sync.Mutex
	file    *os.File
	events  []Event
	records []Event
	byKey   map[string]Event
	acks    map[uint64]bool
	ackKeys map[string]Event
	closed  bool
}

// Open opens or creates an append-only JSONL log and validates every existing
// line before accepting new transitions.
func Open(path string) (*Log, error) {
	if path == "" {
		return nil, fmt.Errorf("write-ahead log path is empty")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open write-ahead log: %w", err)
	}
	log := &Log{file: file, byKey: make(map[string]Event), acks: make(map[uint64]bool), ackKeys: make(map[string]Event)}
	if err := log.readExisting(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return log, nil
}

func (log *Log) readExisting() error {
	if _, err := log.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind write-ahead log: %w", err)
	}
	scanner := bufio.NewScanner(log.file)
	buffer := make([]byte, 0, 64<<10)
	scanner.Buffer(buffer, 16<<20)
	var wantSequence uint64 = 1
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			return fmt.Errorf("empty write-ahead log line")
		}
		var event Event
		if err := json.Unmarshal(line, &event); err != nil {
			return fmt.Errorf("decode write-ahead log sequence %d: %w", wantSequence, err)
		}
		if event.Sequence != wantSequence {
			return fmt.Errorf("write-ahead log sequence %d, want %d", event.Sequence, wantSequence)
		}
		if event.Kind == "ack" {
			if event.Ref == 0 || log.acks[event.Ref] {
				return fmt.Errorf("invalid duplicate write-ahead log acknowledgement")
			}
			log.acks[event.Ref] = true
			log.ackKeys[event.Key] = event
		} else {
			if event.Key == "" || event.Kind == "" || log.byKey[event.Key].Key != "" {
				return fmt.Errorf("invalid or duplicate write-ahead transition key %q", event.Key)
			}
			log.byKey[event.Key] = event
			log.events = append(log.events, event)
		}
		log.records = append(log.records, event)
		wantSequence++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read write-ahead log: %w", err)
	}
	if _, err := log.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek write-ahead log end: %w", err)
	}
	return nil
}

// AppendTransition writes one transition and calls fsync before returning.
// Repeating the same key and content is an idempotent replay; repeating a key
// with different content is rejected.
func (log *Log) AppendTransition(key, jobID, kind string, payload any) (Event, error) {
	if key == "" || kind == "" {
		return Event{}, fmt.Errorf("write-ahead transition key and kind are required")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("encode write-ahead payload: %w", err)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return Event{}, ErrClosed
	}
	if existing, ok := log.byKey[key]; ok {
		if existing.JobID == jobID && existing.Kind == kind && bytes.Equal(existing.Payload, encoded) {
			return existing, nil
		}
		return Event{}, fmt.Errorf("%w: %s", ErrDuplicateKey, key)
	}
	event := Event{Sequence: uint64(len(log.records) + 1), Key: key, JobID: jobID, Kind: kind, Payload: append(json.RawMessage(nil), encoded...)}
	if err := log.appendLocked(event); err != nil {
		return Event{}, err
	}
	log.byKey[key] = event
	log.events = append(log.events, event)
	log.records = append(log.records, event)
	return event, nil
}

// AppendAcknowledgement records that the caller has observed a durable
// transition. Duplicate acknowledgements are idempotent by key.
func (log *Log) AppendAcknowledgement(key string, transition Event) (Event, error) {
	if key == "" || transition.Sequence == 0 {
		return Event{}, fmt.Errorf("write-ahead acknowledgement key and transition are required")
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return Event{}, ErrClosed
	}
	if existing, ok := log.ackKeys[key]; ok {
		if existing.Ref == transition.Sequence {
			return existing, nil
		}
		return Event{}, fmt.Errorf("%w: %s", ErrDuplicateKey, key)
	}
	if !log.acks[transition.Sequence] {
		// The transition may have been written by an older process, but it must
		// still be present in the replayed sequence before it can be acked.
		if _, ok := log.bySequenceLocked(transition.Sequence); !ok {
			return Event{}, fmt.Errorf("acknowledgement references unknown transition %d", transition.Sequence)
		}
	}
	event := Event{
		Sequence: uint64(len(log.records) + 1),
		Key:      key,
		JobID:    transition.JobID,
		Kind:     "ack",
		Ref:      transition.Sequence,
	}
	if err := log.appendLocked(event); err != nil {
		return Event{}, err
	}
	log.ackKeys[key] = event
	log.acks[transition.Sequence] = true
	log.records = append(log.records, event)
	return event, nil
}

func (log *Log) bySequenceLocked(sequence uint64) (Event, bool) {
	for _, event := range log.records {
		if event.Sequence == sequence {
			return event, true
		}
	}
	return Event{}, false
}

func (log *Log) appendLocked(event Event) error {
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode write-ahead event: %w", err)
	}
	if _, err := log.file.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("append write-ahead event: %w", err)
	}
	if err := log.file.Sync(); err != nil {
		return fmt.Errorf("fsync write-ahead event: %w", err)
	}
	return nil
}

// Replay returns independent copies of all durable transitions.
func (log *Log) Replay() Snapshot {
	log.mu.Lock()
	defer log.mu.Unlock()
	events := append([]Event(nil), log.records...)
	acks := make(map[uint64]bool, len(log.acks))
	for sequence := range log.acks {
		acks[sequence] = true
	}
	return Snapshot{Events: events, Acknowledged: acks}
}

// Close fsyncs and closes the log. It is idempotent.
func (log *Log) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return nil
	}
	log.closed = true
	if err := log.file.Sync(); err != nil {
		_ = log.file.Close()
		return err
	}
	return log.file.Close()
}
