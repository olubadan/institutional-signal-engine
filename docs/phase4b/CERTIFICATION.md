# Phase 4B authoritative certification

Run once into a fresh output directory:

```text
uv run python -m institutional_signal_engine.phase4b_certify \
  --output /tmp/phase4b-certification/CERTIFICATE.json \
  --journal-output /tmp/phase4b-certification/JOURNAL.json \
  --persistence-output /tmp/phase4b-certification/PERSISTENCE_RECEIPT.json \
  --replay-output /tmp/phase4b-certification/REPLAY_RECEIPT.json
```

The only authoritative Python interface is:

```text
generate_phase4b_certification(output_paths) -> CertificationArtifacts
```

`output_paths` contains four presentation-only destinations in one directory.
They must be distinct and absent. The operation accepts no contract, contract
path or bytes, journal, receipt, repository, factory, scenario, event,
expectation, callback, observed value, certificate, capability, or result.

## Owned causal lifecycle

One invocation owns the entire lifecycle:

```text
C* = load_and_verify_authoritative_contract()
Sigma* = construct_checked_in_deterministic_scenario()
Repo* = construct_owned_certification_repository()
J = seal(compose(Sigma*))
P = persist(J, Repo*)
R = replay(P, Repo*)
Certificate = project(C*, J, P, R)
Artifacts = publish(J, P, R, Certificate)
```

The authoritative contract is the fixed package resource
`src/institutional_signal_engine/resources/phase4b_acceptance_contract.json`.
The operation reads its exact bytes, verifies the code-pinned SHA-256
`a68e3d894bd29e1ace5dd84dfefab87fe8609c2adae93e9c640c105426e23c9a`
before parsing, and strictly validates its complete structure. No environment,
working directory, explicit path, symlink, or alternate file can select it.

The checked-in scenario has version
`PHASE4B_ACCELERATED_CAUSAL_RTH_V3` and canonical digest
`05ac756c035d8aca12fe8ff5d9016024ef814b512166c8ceecaed40a318b593f`.
It retains the exact `{A} -> {A,B} -> {B}` epochs, paired TRADE/QUOTE
acknowledgements, acknowledgement-gated activation, clock-driven reevaluation,
disconnect/restoration, removed-A rejection, stop, drain, and terminal
finalization.

J comes only from the real owned `OrchestrationShell` execution. The operation
seals and verifies J, saves its exact canonical bytes to an internally
constructed content-addressed repository, and issues P only after save returns.
It reloads P's exact identity, byte-compares the result with J, deserializes a
distinct journal, and independently verifies its seal, hashes, chain, closed
semantics, run, contract, count, root, and digest. R is derived only after the
original and reconstructed bytes, roots, counts, and observed projections are
exactly equal.

The certificate is constructed immediately inside that same call from pinned
contract expectations and independently reconstructed observations. It binds:

- contract version and canonical SHA-256;
- scenario version and canonical SHA-256;
- run ID, journal root, seal, canonical-byte SHA-256 and record count;
- persistence identity and receipt digest;
- replay receipt digest, reconstructed root, counts, bytes, projection digests,
  and exact equality;
- observed projection SHA-256;
- certificate schema version and a canonical certificate commitment SHA-256.

All four serialized artifacts are completed and schema-checked before any is
published. They are staged in the common output directory; J, P, and R are
renamed first and the certificate is published last. A publication failure
removes artifacts created by that invocation. Existing or symlink targets are
never overwritten.

## Non-authoritative low-level interfaces

`Journal`, `AcceptanceContract`, `VerifiedJournal`, receipt constructors,
repositories, deserializers, hashing functions, and low-level verification
helpers remain reusable primitives. Any object produced directly through them
is an untrusted, non-authoritative claim. No supported production function
consumes such an object as certification.

`validate_certificate_schema(value)` checks JSON shape only. A copied or manual
certificate-shaped document may pass it; that result is never proof of
execution, persistence, replay, or certification. There is no production
artifact reader that relabels a schema-valid document authoritative.

The complete pre-edit map, attack matrix, and post-implementation red-team
fixed-point evidence are in `CERTIFICATION_AUTHORITY_MAP.md`.

Git head, worktree state, CI, PR state, and review state are external build-
envelope facts. They are intentionally excluded from the runtime certificate.
The operation is hermetic, contacts no provider, keeps trading disabled, and
constructs/submits zero orders.
