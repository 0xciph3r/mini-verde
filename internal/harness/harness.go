// Package harness contains the reproducible chaos scenarios used by the
// one-command demo and the M7 safety gate.
package harness

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/0xciph3r/mini-verde/internal/chaos"
	"github.com/0xciph3r/mini-verde/internal/coordinator"
	"github.com/0xciph3r/mini-verde/internal/job"
	"github.com/0xciph3r/mini-verde/internal/protocol"
	"github.com/0xciph3r/mini-verde/internal/worker"
)

type Summary struct {
	Scenarios            int    `json:"scenarios"`
	Accepted             int    `json:"accepted"`
	Rejected             int    `json:"rejected"`
	Unresolved           int    `json:"unresolved"`
	SafetyViolations     int    `json:"safety_violations"`
	FalseBlames          int    `json:"false_blames"`
	Disputes             int    `json:"disputes"`
	InjectedDrops        uint64 `json:"injected_drops"`
	InjectedDelays       uint64 `json:"injected_delays"`
	ExpectedNoViolations bool   `json:"expected_no_violations"`
}

type serverEntry struct {
	id       string
	server   *httptest.Server
	behavior worker.Behavior
}

// Run executes deterministic network scenarios. The worker pool contains more
// replicas than the active pair so a dropped message can trigger reassignment
// without silently weakening the one-honest-worker assumption.
func Run(ctx context.Context, scenarios int, seed uint64) (Summary, error) {
	if scenarios <= 0 {
		return Summary{}, nil
	}
	limits := protocol.DefaultLimits()
	entries := make([]serverEntry, 0, 8)
	for i := 0; i < 7; i++ {
		behavior := worker.BehaviorHonest
		if i == 6 {
			behavior = worker.BehaviorCorruptLate
		}
		config := worker.DefaultConfig()
		config.Limits = limits
		config.Behavior = behavior
		config.FaultSeed = uint64(i + 1)
		server, err := worker.NewWithConfig("harness-worker-"+itoa(i), config)
		if err != nil {
			closeServers(entries)
			return Summary{}, err
		}
		entries = append(entries, serverEntry{
			id: "harness-worker-" + itoa(i), server: httptest.NewServer(server), behavior: behavior,
		})
	}
	defer closeServers(entries)

	result := Summary{Scenarios: scenarios, ExpectedNoViolations: true}
	for scenario := 0; scenario < scenarios; scenario++ {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		transport, err := chaos.NewTransport(http.DefaultTransport, chaos.Config{
			Seed:        seed + uint64(scenario),
			MaxDelay:    5 * time.Millisecond,
			DropRateBPS: 500,
		})
		if err != nil {
			return result, err
		}
		endpoints := make([]coordinator.Endpoint, 0, len(entries))
		if scenario%5 == 0 {
			endpoints = append(endpoints, coordinator.Endpoint{ID: entries[6].id, URL: entries[6].server.URL})
		}
		for i, entry := range entries {
			if scenario%5 == 0 && i == 6 {
				continue
			}
			endpoints = append(endpoints, coordinator.Endpoint{ID: entry.id, URL: entry.server.URL})
		}
		config := coordinator.DefaultConfig()
		config.Limits = limits
		config.RequestTimeout = 100 * time.Millisecond
		config.JobTimeout = 2 * time.Second
		client, err := coordinator.NewWithConfig(endpoints, &http.Client{Transport: transport}, config)
		if err != nil {
			return result, err
		}
		scenarioResult, runErr := client.Run(ctx, scenarioSpec(uint64(scenario)+1))
		_ = client.Close()
		stats := transport.Stats()
		result.InjectedDrops += stats.Drops
		result.InjectedDelays += stats.Delays
		if runErr != nil {
			result.Unresolved++
			continue
		}
		switch scenarioResult.Status {
		case coordinator.Accepted:
			result.Accepted++
			if err := checkAcceptedState(scenarioResult, uint64(scenario)+1); err != nil {
				result.SafetyViolations++
			}
		case coordinator.Rejected:
			result.Rejected++
		case coordinator.Unresolved:
			result.Unresolved++
		}
		if scenarioResult.Dispute != nil || len(scenarioResult.PairDisputes) != 0 {
			result.Disputes++
		}
		for _, finding := range scenarioResult.Findings {
			if finding.Verdict == protocol.VerdictTimeout {
				continue
			}
			if finding.WorkerID != entries[6].id {
				result.FalseBlames++
			}
		}
	}
	return result, nil
}

func scenarioSpec(seed uint64) job.JobSpec {
	return job.JobSpec{
		Version: job.ProtocolVersion, Seed: seed, Steps: 8,
		InputSize: 2, HiddenSize: 3, OutputSize: 1, BatchSize: 4,
		LearningRate: job.DefaultLearningRate,
		Dataset: job.Dataset{
			Examples: 8,
			Inputs: []float32{
				0.1, -0.2, 0.2, 0.3, -0.3, 0.4, 0.4, -0.5,
				-0.4, 0.6, 0.5, -0.7, -0.6, 0.8, 0.7, -0.9,
			},
			Targets: []float32{0.05, 0.1, -0.05, 0.2, -0.1, 0.3, 0.15, -0.2},
		},
	}
}

func checkAcceptedState(result coordinator.Result, seed uint64) error {
	machine, err := job.NewMachine(scenarioSpec(seed))
	if err != nil {
		return err
	}
	state := machine.InitialState()
	for state.Step < machine.LeafCount()-1 {
		state, err = machine.Step(state)
		if err != nil {
			return err
		}
	}
	hash, err := machine.HashState(state)
	if err != nil {
		return err
	}
	if hash != result.StateHash {
		return fmt.Errorf("accepted state hash differs from canonical replay")
	}
	return nil
}

func closeServers(entries []serverEntry) {
	for _, entry := range entries {
		entry.server.CloseClientConnections()
		entry.server.Close()
	}
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	index := len(digits)
	for value > 0 {
		index--
		digits[index] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[index:])
}
