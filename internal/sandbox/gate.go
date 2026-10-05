// Package sandbox defines the launch attestation required by the worker
// command. Sandboxing is an external deployment boundary; the worker cannot
// prove from inside its own process that its host is contained.
package sandbox

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type Attestation struct {
	Version  string `json:"version"`
	Runtime  string `json:"runtime"`
	Network  string `json:"network"`
	Attested bool   `json:"attested"`
}

// Require fails closed when the worker is not launched with a supervisor
// attestation. The supervisor must place this file inside the sandbox before
// starting the worker and expose only coordinator networking.
func Require(required bool, path string) error {
	if !required {
		return nil
	}
	if path == "" {
		return fmt.Errorf("sandbox attestation is required")
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("read sandbox attestation: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("sandbox attestation is not a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read sandbox attestation: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var attestation Attestation
	if err := decoder.Decode(&attestation); err != nil {
		return fmt.Errorf("decode sandbox attestation: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("sandbox attestation must contain one JSON value")
	}
	if attestation.Version != "mini-verde/sandbox/v1" || !attestation.Attested {
		return fmt.Errorf("sandbox attestation is not valid")
	}
	if attestation.Runtime != "gvisor" && attestation.Runtime != "container" {
		return fmt.Errorf("sandbox runtime %q is unsupported", attestation.Runtime)
	}
	if attestation.Network != "coordinator-only" {
		return fmt.Errorf("sandbox network policy %q is unsupported", attestation.Network)
	}
	return nil
}
