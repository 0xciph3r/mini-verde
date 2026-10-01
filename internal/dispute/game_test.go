package dispute

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
)

func TestBisectionFindsAdjacentTransitionWithinBound(t *testing.T) {
	for finalStep := uint64(1); finalStep <= 500; finalStep++ {
		for divergence := uint64(1); divergence <= finalStep; divergence++ {
			initial := stepDigest(0, 0)
			leftFinal := stepDigest(0, finalStep)
			rightFinal := stepDigest(1, finalStep)
			game, err := New(finalStep, initial, [2]Digest{leftFinal, rightFinal})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			for {
				index, more, err := game.Next()
				if err != nil {
					t.Fatalf("Next() error = %v", err)
				}
				if !more {
					break
				}
				leftHash := stepDigest(0, index)
				rightHash := leftHash
				if index >= divergence {
					rightHash = stepDigest(1, index)
				}
				if err := game.Observe(index, [2]Digest{leftHash, rightHash}); err != nil {
					t.Fatalf("Observe() error = %v", err)
				}
			}
			transition, err := game.Transition()
			if err != nil {
				t.Fatalf("Transition() error = %v", err)
			}
			if transition.HighStep != divergence {
				t.Fatalf("T=%d divergence=%d high=%d", finalStep, divergence, transition.HighStep)
			}
			if transition.Rounds > CeilLog2(finalStep) {
				t.Fatalf("T=%d rounds=%d, bound=%d", finalStep, transition.Rounds, CeilLog2(finalStep))
			}
		}
	}
}

func stepDigest(trace byte, step uint64) Digest {
	var result Digest
	result[0] = trace
	for i := 0; i < 8; i++ {
		result[i+1] = byte(step >> (8 * i))
	}
	return result
}

func TestBisectionAllowsReconvergenceAndNeedNotFindFirstTransition(t *testing.T) {
	const finalStep = uint64(8)
	left := trace(finalStep, func(step uint64) string { return fmt.Sprintf("canonical-%d", step) })
	right := trace(finalStep, func(step uint64) string {
		switch {
		case step == 0:
			return "canonical-0"
		case step < 4:
			return fmt.Sprintf("first-divergence-%d", step)
		case step < 6:
			return fmt.Sprintf("canonical-%d", step)
		default:
			return fmt.Sprintf("second-divergence-%d", step)
		}
	})
	transition := runGame(t, left, right)
	if transition.LowStep != 5 || transition.HighStep != 6 {
		t.Fatalf("transition = %d->%d, want 5->6", transition.LowStep, transition.HighStep)
	}
}

func TestInjectedRoundCounterCorruptionFailsWithoutAWorkerVerdict(t *testing.T) {
	left := digest("left-final")
	right := digest("right-final")
	game, err := New(8, digest("initial"), [2]Digest{left, right})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	game.rounds = game.maxRounds
	if _, _, err := game.Next(); !errors.Is(err, ErrRoundLimit) {
		t.Fatalf("Next() error = %v, want ErrRoundLimit", err)
	}
}

func runGame(t *testing.T, left, right []Digest) Transition {
	t.Helper()
	game, err := New(uint64(len(left)-1), left[0], [2]Digest{left[len(left)-1], right[len(right)-1]})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for {
		index, more, err := game.Next()
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		if !more {
			break
		}
		if err := game.Observe(index, [2]Digest{left[index], right[index]}); err != nil {
			t.Fatalf("Observe() error = %v", err)
		}
	}
	transition, err := game.Transition()
	if err != nil {
		t.Fatalf("Transition() error = %v", err)
	}
	if left[transition.LowStep] != right[transition.LowStep] {
		t.Fatal("low endpoint does not agree")
	}
	if left[transition.HighStep] == right[transition.HighStep] {
		t.Fatal("high endpoint does not disagree")
	}
	return transition
}

func trace(finalStep uint64, value func(uint64) string) []Digest {
	result := make([]Digest, finalStep+1)
	for step := range result {
		result[step] = digest(value(uint64(step)))
	}
	return result
}

func digest(value string) Digest {
	return sha256.Sum256([]byte(value))
}
