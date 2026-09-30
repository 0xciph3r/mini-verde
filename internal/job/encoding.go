package job

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/0xciph3r/mini-verde/internal/repops"
)

const stateEncodingVersion uint16 = 1

var stateMagic = [4]byte{'M', 'V', 'S', 'T'}

// MarshalState returns the canonical, restart-safe state representation.
func (m *Machine) MarshalState(state State) ([]byte, error) {
	if err := m.validateState(state); err != nil {
		return nil, err
	}
	capacity := len(stateMagic) + 2 + sha256.Size + 8 + 4*8 + 4*m.paramCount
	encoded := make([]byte, 0, capacity)
	encoded = append(encoded, stateMagic[:]...)
	encoded = appendUint16(encoded, stateEncodingVersion)
	encoded = append(encoded, m.id[:]...)
	encoded = appendUint64(encoded, state.Step)
	for _, group := range [][]float32{
		state.Parameters.W1,
		state.Parameters.B1,
		state.Parameters.W2,
		state.Parameters.B2,
	} {
		encoded = appendUint64(encoded, uint64(len(group)))
		for _, value := range group {
			encoded = appendUint32(encoded, math.Float32bits(value))
		}
	}
	return encoded, nil
}

// UnmarshalState validates and decodes a canonical state for this job.
func (m *Machine) UnmarshalState(encoded []byte) (State, error) {
	reader := bytes.NewReader(encoded)
	var magic [4]byte
	if _, err := io.ReadFull(reader, magic[:]); err != nil || magic != stateMagic {
		return State{}, fmt.Errorf("%w: bad state magic", ErrInvalidState)
	}
	version, err := readUint16(reader)
	if err != nil || version != stateEncodingVersion {
		return State{}, fmt.Errorf("%w: bad state encoding version", ErrInvalidState)
	}
	var jobID [sha256.Size]byte
	if _, err := io.ReadFull(reader, jobID[:]); err != nil || jobID != m.id {
		return State{}, fmt.Errorf("%w: state belongs to another job", ErrInvalidState)
	}
	step, err := readUint64(reader)
	if err != nil {
		return State{}, fmt.Errorf("%w: missing step", ErrInvalidState)
	}

	backing := make([]float32, m.paramCount)
	parameters := m.parametersFrom(backing)
	groups := []struct {
		name   string
		values []float32
	}{
		{name: "W1", values: parameters.W1},
		{name: "B1", values: parameters.B1},
		{name: "W2", values: parameters.W2},
		{name: "B2", values: parameters.B2},
	}
	for _, group := range groups {
		length, err := readUint64(reader)
		if err != nil || length != uint64(len(group.values)) {
			return State{}, fmt.Errorf("%w: bad %s length", ErrInvalidState, group.name)
		}
		for i := range group.values {
			bits, err := readUint32(reader)
			if err != nil {
				return State{}, fmt.Errorf("%w: truncated %s", ErrInvalidState, group.name)
			}
			value := math.Float32frombits(bits)
			if !repops.IsFinite(value) {
				return State{}, fmt.Errorf("%w: %s[%d]: %w", ErrInvalidState, group.name, i, ErrNonFinite)
			}
			group.values[i] = value
		}
	}
	if reader.Len() != 0 {
		return State{}, fmt.Errorf("%w: trailing state bytes", ErrInvalidState)
	}
	state := State{Step: step, Parameters: parameters}
	if err := m.validateState(state); err != nil {
		return State{}, err
	}
	return state, nil
}

// HashState hashes the canonical encoding and binds it to the job and step.
func (m *Machine) HashState(state State) ([sha256.Size]byte, error) {
	encoded, err := m.MarshalState(state)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("mini-verde/state/v1\x00"))
	_, _ = hasher.Write(encoded)
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	return digest, nil
}

func readUint16(reader *bytes.Reader) (uint16, error) {
	var encoded [2]byte
	_, err := io.ReadFull(reader, encoded[:])
	return binary.LittleEndian.Uint16(encoded[:]), err
}

func readUint32(reader *bytes.Reader) (uint32, error) {
	var encoded [4]byte
	_, err := io.ReadFull(reader, encoded[:])
	return binary.LittleEndian.Uint32(encoded[:]), err
}

func readUint64(reader *bytes.Reader) (uint64, error) {
	var encoded [8]byte
	_, err := io.ReadFull(reader, encoded[:])
	return binary.LittleEndian.Uint64(encoded[:]), err
}
