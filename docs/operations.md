# Operations and security boundaries

## Sandboxed workers

The worker command fails closed unless `--sandbox-attestation` points to a
supervisor-provided JSON file. The file must have this shape:

```json
{
  "version": "mini-verde/sandbox/v1",
  "runtime": "gvisor",
  "network": "coordinator-only",
  "attested": true
}
```

The supervisor, not the worker, is responsible for enforcing the claim: run
the process in gVisor or a container, deny all network access except the
coordinator, and place the attestation inside the sandbox before starting the
worker. The in-process check is deliberately described as an attestation gate;
a process cannot prove its own host containment from inside the host.

For a local protocol test that uses the reusable `internal/worker` package,
the gate is not involved. The command-line worker remains fail-closed by
default:

```text
go run ./cmd/worker --id worker-a --sandbox-attestation /run/mini-verde/attestation.json
```

## Cross-architecture reproduction

Cross-architecture testing is diagnostic, not a tolerance rule. The protocol
accepts only exact canonical hash equality. A platform that produces different
bits is incompatible and must not be admitted as an honest worker; do not add
an epsilon comparison to the safety path. The checked-in arm64 golden trace is
already verified by the amd64 test path. Repeating that check on independent
hardware or CI runners is optional follow-up work.

## Durable coordinator runs

Use `--wal /path/to/coordinator.jsonl` with the coordinator. Every transition
is fsynced before its acknowledgement record is written. The JSONL file is
human-readable crash evidence; do not edit it in place. Reopening the same WAL
replays terminal results without dispatching new workers. An interrupted
non-terminal run is restarted from its durable job-start record with fresh
attempt IDs.
