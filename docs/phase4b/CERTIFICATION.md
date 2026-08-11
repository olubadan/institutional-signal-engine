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
Certificate = certify_repository_backed(CΔ, J, P, R, repository)
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

The sole production certificate function receives the repository used by
persistence. On every call it independently revalidates sealed J, treats P and R
only as untrusted claims, reloads P's exact object, enforces exact canonical-byte
equality with J, reconstructs and fully verifies a distinct journal, derives the
observed projection from that reconstruction, independently derives all replay
facts, compares caller-supplied R against them, and immediately constructs the
certificate from those verified local results. No intermediate object authorizes
certificate construction; neither receipt can establish persistence or replay
by self-consistency alone.

Every record has a unique operation identity, exact cause-operation identity,
and a stable correlation identity. Validation checks the exact parent instance
and the applicable epoch, command, acknowledgement, boundary, disconnect cycle,
membership, payload, and lifecycle relationship. Unknown, duplicate,
contradictory, misplaced, or unexpected material observations fail even when an
attacker coherently recalculates every structural hash.

`certify_repository_backed(CΔ, J, P, R, repository)` is the only production
boundary that can create `overall: PASS`. There is no capability class, issuance
method, authority token, standalone projection, or repository-free certificate
constructor. `validate_certificate_schema` checks JSON shape only and never
represents persistence, replay, execution, or authoritative issuance. The
certificate keeps expected contract values visibly separate from observed J/P/R
facts. Git head, worktree status, GitHub CI, and pull-request state are separate
build-envelope evidence and never enter the runtime certificate. The command is
hermetic, runs no live provider, keeps trading disabled, and constructs/submits
no orders.
