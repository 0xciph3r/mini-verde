# mini-verde

Mini-Verde is a small, auditable verification protocol for computation carried
out by untrusted workers.

It uses a deterministic float32 training machine, Merkle commitments, an
optimistic bisection dispute game, trusted referee re-execution, worker
reassignment, durable coordinator recovery, and seeded chaos testing. The
project is deliberately small enough to read end-to-end while connecting the
implementation to real distributed-systems concerns: reproducibility, failure
detection, safety versus liveness, durable state machines, idempotence, and
Byzantine fault evidence.

> Mini-Verde is a learning and interview project, not a production distributed
> training system. Its value is in making the invariants and trade-offs
> explicit.

## What problem does it solve?

The coordinator wants to accept the result of a job executed by workers it does
not fully trust. Running the whole job itself defeats the point of delegation,
but trusting one worker is unsafe.

Mini-Verde gives the same canonical job to two workers:

1. Honest workers produce the same final-state hash.
2. A disagreement triggers a Merkle-backed bisection game over the state trace.
3. The coordinator narrows the disagreement to one adjacent transition.
4. The trusted referee re-runs exactly that step from the agreed state.
5. A worker with an objectively invalid step, proof, or state is assigned a
   precise verdict; the verified peer may be accepted under the stated trust
   assumption.

The core safety claim is conditional: when at least one worker in the accepted
pair is honest, the job specification is correct, and SHA-256 is
collision-resistant, a wrong result is not accepted. Collusion between all
workers in a pair and shared implementation bugs are outside that claim.

## Why deterministic computation matters

Mini-Verde uses a tiny two-layer regression MLP trained with plain mini-batch
SGD. The canonical machine fixes:

- the job specification, dataset, initialization, batch selection, and step
  index;
- loop order for forward and backward passes;
- explicit float32 rounding, including the SGD update;
- raw little-endian float32 state encoding;
- rejection of NaN and infinity after every step.

This is a security property, not only a testing convenience. If two honest
workers reduce gradients in different orders, they can produce different bits
and the coordinator cannot distinguish hardware variation from cheating without
weakening comparisons to tolerances.

## Architecture

```text
                    +----------------------+
                    |   trusted coordinator |
                    | job, WAL, referee,    |
                    | bisection, recovery   |
                    +----------+-----------+
                               |
                  HTTP/JSON over loopback
                     +---------+---------+
                     |                   |
              +------+-----+       +-----+------+
              | untrusted  |       | untrusted  |
              | worker A   |       | worker B   |
              +------------+       +------------+
```

Every worker message carries `job_id`, `attempt_id`, and `round`. Worker traces
remain committed until the coordinator closes the attempt. Exact retries are
safe because worker requests are idempotent on identical request bytes; stale
attempts remain fenced by their attempt ID.

The implementation is organized as:

```text
cmd/coordinator/       coordinator CLI
cmd/worker/            worker CLI and startup security gate
cmd/demo/              1,000-scenario chaos demonstration
internal/job/          canonical machine, state encoding, state hashes
internal/repops/       explicitly rounded float32 operations
internal/merkle/       domain-separated Merkle trees and proofs
internal/dispute/      agreed-low/disagreed-high bisection game
internal/coordinator/  HTTP protocol, referee, recovery, verifiers
internal/worker/       worker server and deterministic fault behaviors
internal/chaos/        seeded delay/drop HTTP transport
internal/harness/      chaos scenarios and invariant checks
internal/wal/          append-only JSONL write-ahead log
internal/sandbox/      fail-closed launch attestation gate
```

## Protocol flow

### Agreement

Each worker executes the canonical job and returns a Merkle root plus final
state hash. Matching final-state hashes are not accepted blindly: the
coordinator retrieves a final state, checks its canonical encoding and hash,
then runs the configured verifier. Strict verification is the default and
replays the candidate from the initial state independently.

### Disagreement

The coordinator first opens the final leaf. It then maintains:

```text
step 0       ...       low       low+1       ...       high       ...       T
agreed                 agreed                         disagreed
```

Midpoint hash/proof agreement moves `low` upward; disagreement moves `high`
downward. The trace need not have monotonic disagreement: a
diverge–reconverge–diverge trace is valid. The game finds an adjacent transition,
not necessarily the first divergence. The referee retrieves the state at `low`,
checks its hash, and re-runs one canonical step.

Verdicts are precise: `WrongStep`, `BadOpening`, `BadState`, `Timeout`, and
`BothWrong`. Timeout is suspicion, never computational evidence. A proven
fault can permit acceptance of the peer only after that peer's final opening
and final state verify.

### Recovery and failure handling

The coordinator uses partial-synchrony semantics:

- request deadlines and an overall job deadline are coordinator-owned;
- availability failures get bounded exact retries before timeout attribution;
- a timed-out worker is replaced from an ordered, non-reused worker pool;
- retries and close operations are idempotent;
- the coordinator returns `Unresolved` when liveness evidence is insufficient;
- computational blame is never inferred from timeout alone.

The optional strict verifier interface also includes a two-honest-of-three
quorum policy. That policy is not a magic collusion defense: it only has meaning
when the three checker identities and incentives are independent.

## Quick start

### Prerequisites

- Go 1.26 or newer;
- a loopback-capable environment for the HTTP integration tests;
- no third-party Go dependencies.

Check the build and run the complete test suite:

```sh
go test ./...
```

Run the race detector for the concurrency and protocol gates:

```sh
go test -race ./...
```

### Run the reproducibility and chaos demo

This is the fastest way to see the finished project. It starts in-process
workers, injects seeded delay and 5% message drops, exercises honest and
corrupt-late workers, and independently replays every accepted result.

```sh
go run ./cmd/demo -scenarios 1000 -seed 1
```

The results table should report 1,000 accepted jobs, zero unresolved jobs, zero
safety violations, and zero false blame for the checked-in seed.

### Run two workers and the coordinator over HTTP

The command-line worker is fail-closed by default. A real deployment must run
it under gVisor or a container with coordinator-only networking and provide a
supervisor attestation. For an explicitly unconfined local protocol test, use
`--require-sandbox=false`; do not use that setting for a security deployment.

Start two workers in separate terminals:

```sh
go run ./cmd/worker \
  --id worker-a \
  --listen 127.0.0.1:8081 \
  --require-sandbox=false \
  --execution-workers=4
```

```sh
go run ./cmd/worker \
  --id worker-b \
  --listen 127.0.0.1:8082 \
  --require-sandbox=false \
  --execution-workers=4
```

Submit the example job from a third terminal:

```sh
mkdir -p .run
go run ./cmd/coordinator \
  --job examples/job.json \
  --worker worker-a=http://127.0.0.1:8081 \
  --worker worker-b=http://127.0.0.1:8082 \
  --wal .run/coordinator.jsonl
```

The coordinator prints JSON containing the status, job ID, final-state hash,
worker roots, attempts, dispute findings, pair generations, replacements, and
reputation events.

To exercise the dispute path, start one worker with a deterministic fault:

```sh
go run ./cmd/worker \
  --id worker-b \
  --listen 127.0.0.1:8082 \
  --require-sandbox=false \
  --behavior corrupt-late \
  --fault-seed 91
```

The coordinator should preserve the honest result and report a precise
`WrongStep` finding for the faulty worker.

For the deployment attestation format and WAL recovery behavior, see
[docs/operations.md](docs/operations.md).

## Testing and verification gates

The project has completed M1–M7:

| Milestone | Demonstrates |
| --- | --- |
| M1 | Canonical float32 machine, finite-state checks, gradient check, loss test, restart-safe encoding, arm64/amd64 golden trace |
| M2 | Domain-separated Merkle tree, RFC 6962 split shape, proof verification and fuzz/differential tests |
| M3 | Two-worker HTTP protocol, admission limits, idempotence, checkpoint replay, final-state verification |
| M4 | Merkle-backed bisection, one-step referee, exact fault verdicts, bounded rounds and cost accounting |
| M5 | Fault behaviors, deadlines, reassignment, cancellation attribution, cleanup, reputation events |
| M6 | Per-example parallel buffers, deterministic reduction, naive order-dependent baseline, 1/4/16 worker checks |
| M7 | Seeded delay/drop chaos, retries, JSONL WAL, replay recovery, strict verifier, sandbox gate, 1,000-scenario harness |

Useful commands:

```sh
go test -race ./...
go vet ./...
GOARCH=amd64 go test ./...
```

The design is documented through the implementation, package tests, and
the operational notes in [docs/operations.md](docs/operations.md).

## Security model and boundaries

Trusted:

- the coordinator and referee implementation;
- the correctness of the job specification;
- collision resistance of SHA-256;
- the external supervisor enforcing the worker sandbox.

Not provided:

- confidentiality: the protocol uses integrity checks, not encryption;
- consensus or coordinator fault tolerance;
- protection against all workers sharing the same implementation bug;
- protection against two colluding workers in a pair;
- tolerance-based cross-architecture acceptance.

The worker sandbox check is an external launch gate, not proof that a process
can contain itself. Cross-architecture reproduction is an optional diagnostic
integration; a platform that produces different canonical bits must be rejected
from the honest-worker pool rather than compared with an epsilon.

## Project status and next steps

The core build is complete and committed. The remaining work is optional or
operational:

- run the worker under a real gVisor/container supervisor;
- repeat exact reproduction on independent physical amd64 and arm64 machines;
- wire live three-candidate quorum execution if the trust model justifies it;
- publish a short write-up of the determinism and failure-detection lessons.
