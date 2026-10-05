// Package chaos injects deterministic network delay and loss at the HTTP
// transport boundary. It does not inspect or mutate protocol payloads.
package chaos

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

var ErrDropped = errors.New("chaos dropped HTTP message")

type Config struct {
	Seed        uint64
	MaxDelay    time.Duration
	DropRateBPS uint32
}

func (config Config) validate() error {
	if config.MaxDelay < 0 {
		return fmt.Errorf("chaos maximum delay must not be negative")
	}
	if config.DropRateBPS > 10_000 {
		return fmt.Errorf("chaos drop rate must be at most 10000 basis points")
	}
	return nil
}

// Transport is a deterministic RoundTripper wrapper. The request ordinal is
// part of the random material, so a fixed seed and request sequence reproduce
// the same injected faults. Request cancellation always wins over delay.
type Transport struct {
	base   http.RoundTripper
	seed   uint64
	delay  time.Duration
	drop   uint32
	seq    atomic.Uint64
	drops  atomic.Uint64
	delays atomic.Uint64
}

func NewTransport(base http.RoundTripper, config Config) (*Transport, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if base == nil {
		base = http.DefaultTransport
	}
	return &Transport{base: base, seed: config.Seed, delay: config.MaxDelay, drop: config.DropRateBPS}, nil
}

func (transport *Transport) RoundTrip(request *http.Request) (*http.Response, error) {
	ordinal := transport.seq.Add(1) - 1
	digest := transport.material(request, ordinal)
	if transport.delay > 0 {
		delay := time.Duration(binary.LittleEndian.Uint64(digest[8:16]) % uint64(transport.delay+1))
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
				transport.delays.Add(1)
			case <-request.Context().Done():
				if !timer.Stop() {
					<-timer.C
				}
				return nil, request.Context().Err()
			}
		}
	}
	if uint32(binary.LittleEndian.Uint32(digest[:4])%10_000) < transport.drop {
		transport.drops.Add(1)
		return nil, ErrDropped
	}
	return transport.base.RoundTrip(request)
}

func (transport *Transport) material(request *http.Request, ordinal uint64) [sha256.Size]byte {
	hasher := sha256.New()
	var encoded [16]byte
	binary.LittleEndian.PutUint64(encoded[:8], transport.seed)
	binary.LittleEndian.PutUint64(encoded[8:], ordinal)
	_, _ = hasher.Write([]byte("mini-verde/chaos/v1\x00"))
	_, _ = hasher.Write(encoded[:])
	_, _ = io.WriteString(hasher, request.Method)
	_, _ = io.WriteString(hasher, "\x00")
	_, _ = io.WriteString(hasher, request.URL.String())
	return sha256.Sum256(hasher.Sum(nil))
}

type Stats struct {
	Requests int
	Drops    uint64
	Delays   uint64
}

func (transport *Transport) Stats() Stats {
	return Stats{Requests: int(transport.seq.Load()), Drops: transport.drops.Load(), Delays: transport.delays.Load()}
}

var _ http.RoundTripper = (*Transport)(nil)
