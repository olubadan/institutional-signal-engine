# Final evidence-bundle correction handoff

The recovery branch retains one production and deterministic orchestration path.
Its driver independently selects provider events, scheduled reevaluations,
disconnect/reconnect input, or the intake-stop boundary. The journal records the
causal runtime actions through finalization and is sealed with finalization as
its terminal record.

The final certification authority boundary implements exactly:

```text
C* = load_and_verify_authoritative_contract()
Σ* = construct_checked_in_deterministic_scenario()
Repo* = construct_owned_certification_repository()
J = seal(compose(Σ*))
P = persist(J, Repo*)
R = replay(P, Repo*)
Certificate = project(C*, J, P, R)
Artifacts = publish(J, P, R, Certificate)
```

P and R are separate immutable, recalculably digested receipts. Replay reads the
persisted object identified by P, reconstructs a new journal, reruns structural
and contract-driven semantic verification, and compares canonical bytes, record
counts, root digests, and observed projection digests before R exists.
The sole production function is
`generate_phase4b_certification(output_paths)`. It constructs C*, the exact
scenario, the repository, J, P, R, and the certificate within one call. It
accepts no semantic value or dependency-injection point. Every invocation
independently validates J, reloads P's object, compares exact bytes,
reconstructs and fully verifies a distinct journal, derives observations and
replay facts from verified local results, and immediately constructs the
certificate. No externally supplied object can authorize construction.

The closed semantic language requires exact operation/cause/correlation
identities, exact epoch diffs, paired command acknowledgements, activation only
after all required acknowledgements, active-event membership, the scheduled
clock sequence, disconnect-cycle restoration membership, and the terminal
stop/drain/finalization chain. CΔ owns expected values and rules; J, P, and R own
observations. Build evidence is separate.

The fixed contract package bytes and checked-in scenario are separately digest-
pinned, and both digests are visible in the certificate. The focused suite
mutates contract fields and bytes, scenario identity, J/P/R semantics,
persistence/replay serialization, artifact destinations, and certificate fields.
It also proves that the former capability, caller-driven issuer, authority,
`_issue`, and projection APIs do not exist; low-level lookalikes, schema-valid
manual fields, copied/mutated evidence, and coherent alternate packages have no
authoritative production consumer.

Runtime artifacts:

- `/tmp/phase4b-certification/JOURNAL.json`
- `/tmp/phase4b-certification/PERSISTENCE_RECEIPT.json`
- `/tmp/phase4b-certification/REPLAY_RECEIPT.json`
- `/tmp/phase4b-certification/CERTIFICATE.json`

Providers are not run; trading is disabled; orders remain `0/0`.

Final complete-authority verification contains 65 focused and 279 complete
hermetic tests. Two post-implementation red-team enumerations found no new
authoritative bypass. Two independent executions produced byte-identical J, P,
R, and certificate artifacts with SHA-256 values
`02fa0820152556db2b6a3620224a77865238a393e83536a39399d886406522c1`,
`c97d148f7bbae485be231df2d45714ee749ac80a7de58c3a56a2e96142369835`,
`5c8faad0f1c396d3c4ad00965365a4ca156233e89b112e06f3a469b352f524b1`,
and `e5e1286264614184c7a80cbe23426eff4b0ae7616fb57a2588230679d36ce5e8`.
The contract and scenario digests are respectively
`a68e3d894bd29e1ace5dd84dfefab87fe8609c2adae93e9c640c105426e23c9a`
and `05ac756c035d8aca12fe8ff5d9016024ef814b512166c8ceecaed40a318b593f`.
