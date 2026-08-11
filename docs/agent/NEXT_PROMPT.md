# Proposed Next Codex Prompt

Independently validate the exact-head Phase 4B evidence-bundle correction on
draft PR #6. Confirm `J = seal(compose(E_det, Sigma))`, `P = persist(J)`,
`R = replay(P)`,
`Omega_verified = verify_repository_backed(CDelta, J, P, R, repository)`, and
`Certificate = pi_CDelta(Omega_verified)`. Confirm that the production verifier
receives the persistence repository, reloads P's exact object, reconstructs a
distinct journal, independently verifies all structural, semantic, run,
contract, count, root, byte, and projection facts, independently derives replay
facts, and treats caller-supplied R only as a claim to compare. Confirm that only
the repository-backed verifier can issue the package accepted by projection,
then reproduce the published J/P/R/certificate hashes and exact-head CI. Do not
modify PR #5, mark PR #6 ready, run providers, enable trading, create orders,
merge, or begin Phase 5.
