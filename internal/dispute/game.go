// Package dispute implements the transport-independent midpoint bisection
// state machine used to locate an agreed-to-disagreed transition.
package dispute

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math/bits"
)

var (
	ErrInvalidGame = errors.New("invalid dispute game")
	ErrRoundLimit  = errors.New("dispute round limit exceeded")
	ErrObservation = errors.New("unexpected dispute observation")
)

type Digest = [sha256.Size]byte

// Transition is an adjacent low state shared by both traces and high states
// on which they disagree.
type Transition struct {
	LowStep    uint64
	HighStep   uint64
	AgreedHash Digest
	HighHashes [2]Digest
	Rounds     uint64
}

// Game maintains the agreed-low/disagreed-high invariant. Cryptographic proof
// validation happens before an observation enters the game.
type Game struct {
	low          uint64
	high         uint64
	agreedHash   Digest
	highHashes   [2]Digest
	rounds       uint64
	maxRounds    uint64
	pendingIndex uint64
	pending      bool
}

func New(finalStep uint64, initialHash Digest, finalHashes [2]Digest) (*Game, error) {
	if finalStep == 0 {
		return nil, fmt.Errorf("%w: final step must be positive", ErrInvalidGame)
	}
	if finalHashes[0] == finalHashes[1] {
		return nil, fmt.Errorf("%w: final hashes must disagree", ErrInvalidGame)
	}
	return &Game{
		low: 0, high: finalStep, agreedHash: initialHash, highHashes: finalHashes,
		maxRounds: CeilLog2(finalStep),
	}, nil
}

// Next returns the next midpoint. A false second result means the invariant's
// endpoints are adjacent and Transition is available.
func (game *Game) Next() (uint64, bool, error) {
	if game.pending {
		return 0, false, fmt.Errorf("%w: previous midpoint has no observation", ErrObservation)
	}
	if game.high == game.low+1 {
		return 0, false, nil
	}
	if game.rounds >= game.maxRounds {
		return 0, false, ErrRoundLimit
	}
	game.pendingIndex = game.low + (game.high-game.low)/2
	game.pending = true
	return game.pendingIndex, true, nil
}

// Observe advances exactly the midpoint most recently returned by Next.
func (game *Game) Observe(index uint64, hashes [2]Digest) error {
	if !game.pending || index != game.pendingIndex {
		return fmt.Errorf("%w: index %d", ErrObservation, index)
	}
	game.pending = false
	game.rounds++
	if hashes[0] == hashes[1] {
		game.low = index
		game.agreedHash = hashes[0]
		return nil
	}
	game.high = index
	game.highHashes = hashes
	return nil
}

func (game *Game) Transition() (Transition, error) {
	if game.pending || game.high != game.low+1 {
		return Transition{}, fmt.Errorf("%w: endpoints are not adjacent", ErrInvalidGame)
	}
	return Transition{
		LowStep: game.low, HighStep: game.high, AgreedHash: game.agreedHash,
		HighHashes: game.highHashes, Rounds: game.rounds,
	}, nil
}

// CeilLog2 returns ceil(log2(value)) for positive values.
func CeilLog2(value uint64) uint64 {
	if value <= 1 {
		return 0
	}
	return uint64(bits.Len64(value - 1))
}
