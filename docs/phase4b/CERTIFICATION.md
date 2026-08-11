# Phase 4B Work Package 1 certification

Command: `uv run python -m institutional_signal_engine.phase4b_certify --output /tmp/phase4b-certification/CERTIFICATE.json`

Contract: `PHASE4B_CERT_V1`, defined by
`PHASE4B_CERT_V1.schema.json`. The accelerated scenario uses a virtual UTC
clock mapped to ET, deterministic provider ports, bounded paired allocation,
offline persistence, and replay. The scenario executor consumes actions in
timestamp order and the production signal composition boundary is invoked with
the deterministic interfaces. Every invariant is trace-derived and carries
its record IDs, expected value, and observed value.

Certificate interpretation: `overall=PASS` means every recorded invariant is
true. At this work package the expected output is `overall=FAIL`; failed
invariants are explicit and the command returns nonzero. `CONTROL_V1` remains
the control and `SHADOW_IMPACT_V1` is evaluated offline only. Numeric delta
without recognized provenance is unscoreable.

Evidence boundaries: this is hermetic certification evidence only. It does not
run providers, validate live discovery, repair the production planner handoff,
or constitute market-observation evidence. The committed
`PHASE4B_CERT_V1.example.json` is not runtime evidence. The runtime certificate
must be written outside the repository, identify the exact checked-out HEAD,
and require a clean worktree for a clean-head result. Trading is disabled and
orders are zero.
