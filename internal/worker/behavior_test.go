package worker

import (
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/0xciph3r/mini-verde/internal/protocol"
)

func TestCrasherInvokesProcessOwnedHook(t *testing.T) {
	var crashes atomic.Uint64
	config := DefaultConfig()
	config.Behavior = BehaviorCrasher
	config.FaultSeed = 9
	config.Crash = func() { crashes.Add(1) }
	server, err := NewWithConfig("worker-a", config)
	if err != nil {
		t.Fatalf("NewWithConfig() error = %v", err)
	}
	response := servePayload(server, protocol.ExecutePath, executePayload(t, 1))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("execute status = %d, want %d", response.Code, http.StatusInternalServerError)
	}
	if got := crashes.Load(); got != 1 {
		t.Fatalf("crash hook calls = %d, want 1", got)
	}
	if server.ActiveAttempts() != 0 {
		t.Fatal("simulated crashed attempt retained state")
	}
}

func TestSlowBehaviorRequiresExplicitPositiveDelay(t *testing.T) {
	config := DefaultConfig()
	config.Behavior = BehaviorSlow
	if _, err := NewWithConfig("worker-a", config); err == nil {
		t.Fatal("NewWithConfig() accepted slow behavior without a delay")
	}
}
