# mini-verde

A small Verde-style verification protocol for untrusted workers: deterministic
SGD, Merkle commitments, bisection disputes, reassignment, durable recovery,
and seeded chaos testing.

The current implementation has completed M1–M7. Run the reproducibility and
chaos demonstration with:

```text
go run ./cmd/demo -scenarios 1000 -seed 1
```

The worker command is fail-closed unless it receives an external sandbox
attestation. See [docs/operations.md](docs/operations.md) for the deployment
boundary, WAL usage, and optional cross-architecture checks. The full design
and milestone record is in [Mini-Verde PRD & Build Plan.md](Mini-Verde%20PRD%20%26%20Build%20Plan.md).
