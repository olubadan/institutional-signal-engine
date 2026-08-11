# Phase 4B evidence-bundle certification

Run:

```text
uv run python -m institutional_signal_engine.phase4b_certify \
  --output /tmp/phase4b-certification/CERTIFICATE.json \
  --journal-output /tmp/phase4b-certification/JOURNAL.json \
  --persistence-output /tmp/phase4b-certification/PERSISTENCE_RECEIPT.json \
  --replay-output /tmp/phase4b-certification/REPLAY_RECEIPT.json
```

The immutable contract `CΔ` is loaded before execution and defines the expected
run identity, versions, closed record vocabulary, exact epoch sequence, clock
boundaries, event observations, restoration membership, lifecycle ordering, and
required invariants. It contains rules and expected values, not observations.

The runtime evidence lifecycle is:

```text
J = seal(compose(E_det, Σ))
P = persist(J)
R = replay(P)
Ω_verified = verify_repository_backed(CΔ, J, P, R, repository)
Certificate = π_CΔ(Ω_verified)
```

`session.finalized` is the terminal record in J. Sealing prohibits later
appends. J contains no persistence-success or replay-success claim. P is issued
only after the exact canonical sealed bytes are saved through the journal
repository, and binds the CΔ version, run identity, root digest, byte digest,
record count, persistence identity, completion status, and its own receipt
digest.

Replay reads P's persistence identity and never reuses the original in-memory
journal. It deserializes a new object, verifies the seal, payload and record
digests, chain, closed semantics, and CΔ run binding, then compares exact bytes,
counts, roots, and observed projections. R binds all original/reconstructed
values, the persistence identity, exact-equality result, and its own receipt
digest. R is not appended to J.

The production evidence verifier receives the same repository used by
persistence. It independently reloads P's exact object, reconstructs and fully
verifies another distinct journal, enforces exact canonical-byte equality with
J, independently derives the replay receipt facts, and compares caller-supplied
R against those facts. P and R are claims until that repository-backed boundary
issues the opaque verified package; neither receipt can establish persistence or
replay by self-consistency alone.

Every record has a unique operation identity, exact cause-operation identity,
and a stable correlation identity. Validation checks the exact parent instance
and the applicable epoch, command, acknowledgement, boundary, disconnect cycle,
membership, payload, and lifecycle relationship. Unknown, duplicate,
contradictory, misplaced, or unexpected material observations fail even when an
attacker coherently recalculates every structural hash.

`π_CΔ(Ω_verified)` receives only immutable CΔ and the repository-backed verified
package. Directly constructed or otherwise unverified packages are rejected. The certificate keeps
expected contract values visibly separate from observed J/P/R facts. Git head,
worktree status, GitHub CI, and pull-request state are separate build-envelope
evidence and never enter the runtime certificate. The command is hermetic, runs
no live provider, keeps trading disabled, and constructs/submits no orders.
