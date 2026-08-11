# Proposed Next Codex Prompt

Independently review the exact-head complete Phase 4B certification-authority
closure on draft PR #6. Begin from the published commit and do not trust builder
claims or artifacts.

Confirm that the only authoritative production interface is
`generate_phase4b_certification(output_paths) -> CertificationArtifacts`; its
input is presentation-only; and one call internally loads and digest-verifies
the fixed package contract, verifies the checked-in scenario digest, constructs
the scenario and repository, executes and seals J, persists and reloads P,
independently reconstructs and verifies R, immediately constructs the
certificate, and publishes J/P/R/certificate only after completion.

Freshly enumerate all certificate/PASS constructors, projections, validators,
receipts, contract/journal loaders, persistence/replay paths, CLIs, writers,
exports, private semantic helpers, environment/path/config/DI points, and
test-only imports. Attempt coherent whole-package, cross-run, cross-contract,
self-rehash, pre-seed, copied-replay, manual-schema, former-capability, object
mutation, output collision/symlink, alternate CLI/writer, and artifact-consumer
bypasses. Verify low-level helpers are non-authoritative and no supported
consumer can relabel their result.

Reproduce the full checks, 65 focused tests, 279 hermetic tests, fixed contract
and scenario digests, and byte-identical J/P/R/certificate hashes. Confirm exact-
head CI, draft/unmerged PR state, unchanged PR #5 at
`4a3afe8dd70fecc1b629ff7ef5614a652a572529`, disabled trading, orders `0/0`, no
provider access, and no Phase 5 work. Do not modify, merge, or claim acceptance
unless the independent review itself supports it.
