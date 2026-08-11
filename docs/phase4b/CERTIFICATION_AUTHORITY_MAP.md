# Phase 4B certification authority map

## Frozen pre-edit threat model — SESSION-0031

This map was frozen at rejected head
`cd4b5890894e97dc3cd062c91844859e8bcd1efd` before production code was
changed. It covers supported Python and CLI interfaces and artifacts. It does
not claim resistance to modification of the running interpreter or checked-in
code.

### Authority classes and trusted computing base

Every value belongs to exactly one class:

1. **Trusted checked-in anchor:** validated code, the single package-relative
   contract resource and its code-pinned SHA-256, the checked-in deterministic
   scenario and its code-pinned digest, the internally constructed repository,
   and the SHA-256 implementation.
2. **Internally observed runtime fact:** a value emitted by the one owned
   orchestration run or independently reconstructed from the exact repository
   object saved by that run.
3. **Untrusted claim requiring independent verification:** every supplied or
   deserialized contract, journal, receipt, repository value, certificate,
   cached result, fixture result, identity, digest, or schema-valid document.
4. **Derived fact:** a deterministic projection or digest of trusted anchors and
   independently verified runtime facts.
5. **External build-envelope fact:** Git head, worktree, CI, PR, and review
   state; these never enter the runtime certificate.
6. **Presentation-only input:** the four output locations accepted by the
   authoritative operation and CLI. They select destinations, never semantics.

Public callers, Python objects, environment values, serialized values, paths,
factories, callbacks, and dependency-injection points are untrusted. The sole
authoritative path must therefore accept no semantic value.

### Pre-edit construction graph

| Construction or consumer | Pre-edit path | Authority defect and required closure |
| --- | --- | --- |
| Certificate / `overall: PASS` | `certify_repository_backed(C,J,P,R,repo)` | Sole literal producer, but every authority is caller-selected. Remove it and construct only inside `generate_phase4b_certification(output_paths)` after the complete owned lifecycle. |
| CLI and writers | `main(argv)` → `execute_composition()` → issuer → four direct writes | Keep one CLI route and presentation paths only; publish only after construction and schema checking finish. |
| Contract loader | `load_acceptance_contract(path=CONTRACT_PATH)` | Caller path, repository-relative resolution, no byte pin. Replace with argument-free package-resource loading, exact-byte digest check, canonical deserialization, and full validation. |
| Composition | public `execute_composition()` → public `Composition(C,J,P,R,reconstructed,repo)` | Returns every reusable authority object. Remove the public result path; owned execution may return only within the top-level operation. |
| Projection | nested projections over supplied/reconstructed `VerifiedJournal` | Keep projection local to the authoritative call; no standalone projection may return a certificate or PASS. |
| Schema validation | `validate_certificate_schema(value)` | Correctly shape-only, but schema path is mutable repository-relative. Keep explicitly non-authoritative and use a fixed checked-in package schema. |
| Journal construction | public `Journal`, `append`, `seal`, `deserialize`, `verify_complete`, directly constructible `VerifiedJournal` | These are untrusted low-level construction/parsing/verification primitives. No production consumer may convert their result into authoritative certification. |
| Persistence | public repositories, `PersistenceReceipt` constructors/deserializer, `persist_verified_journal` | Self-consistent P does not prove save. The authoritative call must instantiate and own the repository, save first, reload the named object, and locally derive P. |
| Replay | `ReplayReceipt` constructors/deserializer and `replay_persisted(C,J,P,repo)` | Self-consistent R does not prove reload. The authoritative call must reload, deserialize a distinct journal, fully verify it, derive both projections, then locally derive R. |
| Public exports | all module globals importable; package `__init__` exports only version | Define an explicit production export surface. Low-level importability never implies authority. |
| DI / callbacks | generic `OrchestrationShell` accepts clock, planner, adapters, repositories, pre-seeded journal, driver, and callback | Required by normal orchestration, but forbidden at the certification boundary. The authoritative operation constructs exact checked-in dependencies itself. |
| Environment / config | application entrypoint reads `RUNTIME_ENV_FILE`; certification creates fixture Settings | Certification must read no environment-selected semantic input and contact no provider. Fixture credentials are not authority facts. |
| Test-only paths | tests import production helpers; production imports no tests | Preserve one-way dependency and structurally prove no production import from `tests`. |

### Certificate-field authority map

| Certificate field(s) | Required class and authority | Substitution to falsify |
| --- | --- | --- |
| `certificate_version`, `evidence_kind`, schema version | derived fact from pinned contract and checked-in format | caller version, manual PASS document, alternate constructor |
| `acceptance_contract.contract_version` | trusted checked-in anchor | caller contract, altered version, cross-contract replacement |
| authoritative contract canonical SHA-256 | derived fact from exact pinned package bytes | missing/malformed/modified bytes, wrong pin, env/CWD/symlink/alternate path |
| expected run, epochs, boundaries, accepted/rejected events, restoration, lifecycle, invariants | trusted checked-in anchor loaded only from pinned contract | omission, mutation, duplication, reduced invariants, coherent alternate contract |
| scenario version and canonical digest | trusted checked-in anchor / derived digest | rebound version, changed membership/signal/timing, caller events |
| `observed.run_id` | internally observed and repository-reconstructed fact | pre-seeded/caller J, cross-run object |
| configuration versions | internally observed fact bound to checked-in scenario and preserved model versions | coherent rehash with altered scenario/control/shadow/coverage |
| epoch sequence/id/membership/operation identity | internally observed fact checked against contract/scenario | omitted/extra/duplicated epoch, changed membership, replacement identities |
| subscription command/ack fields and ordering | internally observed fact | missing/duplicate/mismatched ack, wrong correlation, activation before ack |
| accepted/rejected event fields | internally observed fact checked against exact contract expectations | fabrication, omission, addition, duplication, cross-epoch, removed-contract acceptance |
| clock boundaries and reevaluation count/cause | internally observed fact | event-triggered replacement, omitted/extra/wrong boundary |
| recovery cycle/count/membership | internally observed fact | wrong disconnect/reconnect/restoration or membership |
| lifecycle | internally observed fact | reordered/omitted/duplicated stages or record after finalization |
| trading and orders | internally observed safety fact | trading true or constructed/submitted nonzero |
| journal count/terminal/root/seal/byte SHA-256 | derived from independently verified reconstructed J | unsealed, altered bytes/count/root, coherent false rehash |
| persistence identity and receipt fields/digest | derived only after owned save and exact reload | empty/wrong/caller repo, invented identity, pre-save P, absent object, cross-run/contract object |
| replay roots/bytes/counts/projection digests/equality/receipt digest | derived only after distinct repository reconstruction | invented R, different object, copied J facts, non-distinct reconstruction, self-consistent forgery |
| observed projection digest | derived from reconstructed J | mutation or projection copied from original memory only |
| invariant booleans, failed list, `overall` | derived from the full pinned expected set and reconstructed observations | reduced required set, direct PASS helper, copied/mutated valid result |
| certificate canonical digest | derived over the canonical certificate commitment where format supports it | copied/mutated certificate with stale or self-selected digest |

### Frozen attack matrix

| Fact | Authority | Possible substitution | Required falsification | Stable failure/classification |
| --- | --- | --- | --- | --- |
| Contract bytes and all expected fields | fixed package resource + code pin | caller object/path; altered events, epochs, boundaries, restoration, lifecycle, invariants or versions; missing/malformed bytes; env/CWD/symlink route | no semantic parameter; exact fixed resource read; SHA-256 before parse; full expected-shape validation | `CONTRACT_RESOURCE_MISSING`, `CONTRACT_RESOURCE_INVALID`, `CONTRACT_DIGEST_MISMATCH`, `CONTRACT_DESERIALIZATION_FAILED`, `CONTRACT_VALIDATION_FAILED` |
| Scenario | checked-in deterministic definition + pin | supplied events/membership/expected values, mutated version or digest | no scenario input/factory/callback; recompute and compare pinned digest; compare contract binding | `SCENARIO_DIGEST_MISMATCH`, `SCENARIO_CONTRACT_MISMATCH` |
| J | owned real orchestration result | pre-seeded/caller/cached journal; omitted/extra/fabricated/cross-epoch events; altered ack, clock, restoration, lifecycle; coherent rehash | construct dependencies and Journal internally; seal after the real run; deserialize and fully verify | existing granular structural/semantic codes, including `UNSEALED_JOURNAL`, `MISSING_REQUIRED_RECORD`, `DUPLICATE_RECORD`, `UNEXPECTED_*`, `ACTIVATION_BEFORE_ACK`, `REEVALUATION_CLOCK_CAUSE_INVALID`, `RESTORATION_MEMBERSHIP_INVALID`, `RECORD_AFTER_FINALIZATION` |
| P | owned repository save | empty/wrong/caller repo, factory substitution, invented identity/digest, receipt before save, cross-run/contract object | final owned repository; save exact J bytes; confirm identity; reload exact object before P is accepted | `PERSISTENCE_SAVE_FAILED`, `PERSISTED_JOURNAL_MISSING`, `PERSISTENCE_OBJECT_IDENTITY_INVALID`, `PERSISTED_JOURNAL_BYTES_MISMATCH` |
| R | distinct repository reconstruction | caller R, replay without reload, different object, copied original facts, altered/self-rehashed receipt | reload P identity; byte compare; deserialize distinct J; verify structure/semantics; locally derive and compare all facts | `REPLAY_RECONSTRUCTION_NOT_DISTINCT`, `REPLAY_JOURNAL_BINDING_MISMATCH`, `REPLAY_PROJECTION_MISMATCH`, `REPLAY_EXACT_EQUALITY_FAILED` |
| Certificate / PASS | top-level owned lifecycle only | direct constructor/helper, former capability/lookalike, `object.__new__`, `object.__setattr__`, manual schema-valid JSON, alternate CLI/writer, mutated copy | only one producer; output-path-only signature; no semantic artifacts accepted by any publisher/consumer; AST/public-surface proof | unsupported call raises `TypeError`; manual/schema-valid values remain explicitly `NON_AUTHORITATIVE_SHAPE_ONLY`; no alternate producer exists |
| Artifact files | derived complete J/P/R/certificate | supplied artifact reused as input, alternate writer, duplicate output targets | writers consume only local completed values; API never reads artifacts; validate distinct presentation destinations; publish after full construction | `OUTPUT_PATHS_INVALID`, `OUTPUT_PATHS_NOT_DISTINCT`, `ARTIFACT_PUBLICATION_FAILED` |
| Build envelope | external evidence | Git/CI/PR claims inserted into runtime certificate | exclude fields and schema; record only in journal/PR description | structural rejection by certificate schema |

Every field receives omission, mutation, substitution, duplication where
applicable, cross-run, cross-contract, coherent rehash, alternate construction,
and serialization/replay mutation coverage either through the actual
output-only production boundary or by structural proof that the relevant value
cannot enter that boundary. A direct low-level object may be well-formed; it is
still non-authoritative because no supported production interface consumes it
as certification.

## Post-implementation fixed-point record

The implementation stopped changing before these enumerations. Two consecutive
fresh passes found no new authoritative bypass.

### Pass 1 — API and substitution enumeration

- Runtime and AST enumeration found five intentional exports:
  `CertificationArtifacts`, `CertificationOutputPaths`,
  `generate_phase4b_certification`, `main`, and the shape-only
  `validate_certificate_schema`.
- The authoritative signature has exactly one parameter, `output_paths`.
- Only `generate_phase4b_certification` contains the authoritative certificate
  kind or literal `overall: PASS` construction anywhere under `src/`.
- Former `Composition`, caller-driven certify/replay/load/write functions,
  capabilities, authority objects, and `_issue` are absent.
- Direct attacks using caller contracts, Journal/P/R types and instances,
  repositories, manual PASS dictionaries, lookalikes, copied results, and extra
  semantic arguments all failed structurally with `TypeError` and produced no
  artifact.
- No environment-selected resource, test import, provider adapter, repository
  factory, callback, or alternate certification CLI was found.

Result: `RED_TEAM_PASS_1_NO_NEW_BYPASS`.

### Pass 2 — producer/consumer and I/O reachability enumeration

- The only production caller of the generator is its CLI `main`; the only
  caller of the fixed contract loader is the generator.
- The only construction of the certification repository occurs inside the
  generator.
- Contract and schema readers have no path argument. Artifact staging and
  replacement occur only inside the generator.
- No production certificate loader or consumer exists. A manual PASS artifact
  placed at an output target was rejected as `OUTPUT_TARGET_EXISTS`, remained
  unchanged, and was never relabeled or consumed.
- The schema validator is reachable from the generator for final shape checking,
  but it cannot reach generation, persistence, publication, or a PASS producer.
- Field/projection enumeration again located all certificate semantic fields in
  the generator and no second writer or producer.

Result: `RED_TEAM_PASS_2_GRAPH_NO_NEW_BYPASS` and
`RED_TEAM_PASS_2_CONSUMER_NO_NEW_BYPASS`.

### Final falsification summary

The 65-test focused suite covers fixed-resource absence, malformed/modified
bytes, wrong pins, full contract-field validation, scenario digest and contract
binding, caller semantic arguments, former APIs/capabilities, low-level coherent
packages, object mutation, schema-valid manual documents, CWD/environment
substitution, journal omission/mutation/duplication/coherent rehash, output
collision/existence/symlink/partial publication, artifact copying/mutation,
exact hashes, build-envelope exclusion, no provider/order path, and two complete
byte-identical runs. The full hermetic suite contains 279 tests.

Two independent executions produced identical J/P/R/certificate bytes:

- J: `02fa0820152556db2b6a3620224a77865238a393e83536a39399d886406522c1`
- P: `c97d148f7bbae485be231df2d45714ee749ac80a7de58c3a56a2e96142369835`
- R: `5c8faad0f1c396d3c4ad00965365a4ca156233e89b112e06f3a469b352f524b1`
- certificate artifact: `e5e1286264614184c7a80cbe23426eff4b0ae7616fb57a2588230679d36ce5e8`
- certificate canonical commitment:
  `79289ecf4f0fe89349b0e1f41ea33146f407ef2f323fbbf78b65e9a84c3b00d6`

The contract digest is
`a68e3d894bd29e1ace5dd84dfefab87fe8609c2adae93e9c640c105426e23c9a`;
the scenario digest is
`05ac756c035d8aca12fe8ff5d9016024ef814b512166c8ceecaed40a318b593f`.
Trading is disabled, orders are `0/0`, no provider path is reachable, and no
Phase 5 code or claim was introduced.
