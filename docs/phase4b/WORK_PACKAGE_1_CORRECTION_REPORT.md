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
Certificate = certify_repository_backed(CΔ, J, P, R, repository)
```

P and R are separate immutable, recalculably digested receipts. Replay reads the
persisted object identified by P, reconstructs a new journal, reruns structural
and contract-driven semantic verification, and compares canonical bytes, record
counts, root digests, and observed projection digests before R exists.
Certification then gives the sole fused production function the same repository.
Every invocation independently validates J, reloads P's object, compares exact
bytes, reconstructs and fully verifies a distinct journal, derives observations
and replay facts from verified local results, compares caller-supplied R, and
immediately constructs the certificate. No externally supplied intermediate
object can authorize construction.

The closed semantic language requires exact operation/cause/correlation
identities, exact epoch diffs, paired command acknowledgements, activation only
after all required acknowledgements, active-event membership, the scheduled
clock sequence, disconnect-cycle restoration membership, and the terminal
stop/drain/finalization chain. CΔ owns expected values and rules; J, P, and R own
observations. Build evidence is separate.

The focused suite mutates J, P, R, CΔ, and persisted bytes and always traverses
the fused production boundary. It also proves that the former capability,
authority, `_issue`, and projection APIs do not exist; low-level lookalikes,
schema-valid manual fields, copied/mutated evidence, and repository-load bypasses
cannot issue authoritative certificates. The suite retains the earlier genuine
chain, seal, epoch, command, acknowledgement, event, clock, lifecycle, receipt,
repository-object, run, and contract corruptions.

Runtime artifacts:

- `/tmp/phase4b-certification/JOURNAL.json`
- `/tmp/phase4b-certification/PERSISTENCE_RECEIPT.json`
- `/tmp/phase4b-certification/REPLAY_RECEIPT.json`
- `/tmp/phase4b-certification/CERTIFICATE.json`

Providers are not run; trading is disabled; orders remain `0/0`.
