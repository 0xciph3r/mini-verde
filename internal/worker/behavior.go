package worker

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/0xciph3r/mini-verde/internal/job"
	"github.com/0xciph3r/mini-verde/internal/protocol"
)

type Behavior string

const (
	BehaviorHonest       Behavior = "honest"
	BehaviorCorruptLate  Behavior = "corrupt-late"
	BehaviorCorruptAll   Behavior = "corrupt-all"
	BehaviorLazy         Behavior = "lazy"
	BehaviorLiarOnBisect Behavior = "liar-on-bisect"
	BehaviorStaller      Behavior = "staller"
	BehaviorCrasher      Behavior = "crasher"
	BehaviorSlow         Behavior = "slow"
)

var errInjectedCrash = errors.New("injected worker crash")

// Config controls admission and deterministic M5 fault injection. Crash is
// supplied by the command so the reusable worker package never calls os.Exit.
type Config struct {
	Limits           protocol.Limits
	Behavior         Behavior
	FaultSeed        uint64
	Delay            time.Duration
	ExecutionWorkers int
	Crash            func()
}

func DefaultConfig() Config {
	return Config{Limits: protocol.DefaultLimits(), Behavior: BehaviorHonest, ExecutionWorkers: 1}
}

func (config Config) validate() error {
	if err := config.Limits.Validate(); err != nil {
		return err
	}
	switch config.Behavior {
	case BehaviorHonest, BehaviorCorruptLate, BehaviorCorruptAll, BehaviorLazy,
		BehaviorLiarOnBisect, BehaviorStaller, BehaviorCrasher, BehaviorSlow:
	default:
		return fmt.Errorf("unknown worker behavior %q", config.Behavior)
	}
	if config.Delay < 0 {
		return fmt.Errorf("behavior delay must not be negative")
	}
	if config.ExecutionWorkers <= 0 {
		return fmt.Errorf("execution workers must be positive")
	}
	if config.Behavior == BehaviorSlow && config.Delay == 0 {
		return fmt.Errorf("slow behavior requires a positive delay")
	}
	return nil
}

func (server *Server) faultStep(machine *job.Machine) uint64 {
	return FaultStep(machine, server.behavior, server.faultSeed)
}

// FaultStep returns the deterministic transition targeted by a behavior.
// Zero means that the behavior does not alter canonical execution.
func FaultStep(machine *job.Machine, behavior Behavior, faultSeed uint64) uint64 {
	steps := machine.LeafCount() - 1
	switch behavior {
	case BehaviorCorruptAll:
		return 1
	case BehaviorLazy:
		return steps/2 + 1
	case BehaviorCorruptLate, BehaviorLiarOnBisect, BehaviorCrasher:
		material := make([]byte, 0, len("mini-verde/fault/v1\x00")+sha256.Size+8)
		material = append(material, "mini-verde/fault/v1\x00"...)
		jobID := machine.ID()
		material = append(material, jobID[:]...)
		var seed [8]byte
		binary.LittleEndian.PutUint64(seed[:], faultSeed)
		material = append(material, seed[:]...)
		digest := sha256.Sum256(material)
		return 1 + binary.LittleEndian.Uint64(digest[:8])%steps
	default:
		return 0
	}
}

func corruptState(state *job.State) {
	groups := [][]float32{
		state.Parameters.W1,
		state.Parameters.B1,
		state.Parameters.W2,
		state.Parameters.B2,
	}
	for _, group := range groups {
		if len(group) == 0 {
			continue
		}
		// Flip the top mantissa bit: the value remains finite while the change is
		// large enough not to disappear in the next float32 update.
		group[0] = math.Float32frombits(math.Float32bits(group[0]) ^ 0x00400000)
		return
	}
}
