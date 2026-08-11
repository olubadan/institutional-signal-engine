# Proposed Next Codex Prompt

Independently validate the exact-head fused Phase 4B certification correction on
draft PR #6. Confirm `J = seal(compose(E_det, Sigma))`, `P = persist(J)`,
`R = replay(P)`, and
`Certificate = certify_repository_backed(CDelta, J, P, R, repository)`.
Confirm that every invocation validates sealed J, treats P and R as untrusted
claims, loads P's exact object, compares canonical bytes, reconstructs a distinct
journal, independently verifies structure, closed semantics, run, contract,
count, root and digest, derives observations and replay facts from verified local
results, compares caller R, and constructs the certificate immediately. Confirm
the former capability, `_issue`, authority, projection, and repository-free
construction paths are absent; schema validation is shape-only; repository load
cannot be bypassed; and only the fused boundary can construct authoritative
`overall: PASS`. Reproduce the published J/P/R/certificate hashes and exact-head
CI. Do not modify PR #5, mark PR #6 ready, run providers, enable trading, create
orders, merge, or begin Phase 5.
