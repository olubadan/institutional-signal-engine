# Final evidence-bundle correction handoff

The recovery branch retains one production and deterministic orchestration path.
Its driver independently selects provider events, scheduled reevaluations,
disconnect/reconnect input, or the intake-stop boundary. The journal records the
causal runtime actions through finalization and is sealed with finalization as
its terminal record.

The evidence boundary now implements exactly:

```text
J = seal(compose(E_det, Σ))
P = persist(J)
R = replay(P)
Ω_verified = verify_repository_backed(CΔ, J, P, R, repository)
Certificate = π_CΔ(Ω_verified)
```

P and R are separate immutable, recalculably digested receipts. Replay reads the
persisted object identified by P, reconstructs a new journal, reruns structural
and contract-driven semantic verification, and compares canonical bytes, record
counts, root digests, and observed projection digests before R exists.
Certification then gives the production verifier the same repository, reloads
P's object again, reconstructs and fully verifies a distinct journal, derives
the replay facts independently, and compares caller-supplied R with those facts.
Only that repository-backed verifier can issue the package accepted by the
certificate projection.

The closed semantic language requires exact operation/cause/correlation
identities, exact epoch diffs, paired command acknowledgements, activation only
after all required acknowledgements, active-event membership, the scheduled
clock sequence, disconnect-cycle restoration membership, and the terminal
stop/drain/finalization chain. CΔ owns expected values and rules; J, P, and R own
observations. Build evidence is separate.

The focused suite mutates J, P, R, CΔ, and persisted bytes and always traverses
the production verifier and projection. It covers the requested same-kind
parent, coherent fabrications, duplicates, run and version mismatches,
restoration/boundary errors, forged receipts, unsealed replay, wrong object
bindings, projection inequality, and post-finalization records, while retaining
the earlier genuine chain, seal, epoch, command, acknowledgement, event, clock,
and lifecycle corruptions.

Runtime artifacts:

- `/tmp/phase4b-certification/JOURNAL.json`
- `/tmp/phase4b-certification/PERSISTENCE_RECEIPT.json`
- `/tmp/phase4b-certification/REPLAY_RECEIPT.json`
- `/tmp/phase4b-certification/CERTIFICATE.json`

Providers are not run; trading is disabled; orders remain `0/0`.
