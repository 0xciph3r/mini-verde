package sandbox

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRequireFailsClosedWithoutAttestation(t *testing.T) {
	if err := Require(true, ""); err == nil {
		t.Fatal("Require() accepted missing attestation")
	}
}

func TestRequireAcceptsOnlySupportedCoordinatorOnlyAttestation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attestation.json")
	if err := os.WriteFile(path, []byte(`{"version":"mini-verde/sandbox/v1","runtime":"gvisor","network":"coordinator-only","attested":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Require(true, path); err != nil {
		t.Fatalf("Require() error = %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"version":"mini-verde/sandbox/v1","runtime":"gvisor","network":"any","attested":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Require(true, path); err == nil {
		t.Fatal("Require() accepted an unrestricted network policy")
	}
}
