package harness

import (
	"context"
	"testing"
)

func TestChaosHarnessMaintainsSafetyUnderSeededFaults(t *testing.T) {
	summary, err := Run(context.Background(), 20, 91)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if summary.SafetyViolations != 0 || summary.FalseBlames != 0 {
		t.Fatalf("summary = %+v, want no safety violations or false blame", summary)
	}
	if summary.InjectedDrops == 0 || summary.InjectedDelays == 0 {
		t.Fatalf("summary = %+v, want both chaos injections", summary)
	}
}
