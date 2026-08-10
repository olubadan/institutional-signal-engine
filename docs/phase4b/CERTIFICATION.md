# Phase 4B Work Package 1 certification

Command: `uv run python -m institutional_signal_engine.phase4b_certify`

Contract: `PHASE4B_CERT_V1`, defined by
`PHASE4B_CERT_V1.schema.json`. The accelerated scenario uses a virtual UTC
clock mapped to ET, deterministic provider/acknowledgement fixtures, bounded
paired allocation, offline persistence, and replay. It covers startup, RTH
epoch changes, classification, impact scoring, disconnect recovery, close,
and finalization.

Certificate interpretation: `overall=PASS` means every recorded invariant is
true. At this work package the expected output is `overall=FAIL`; failed
invariants are explicit and the command returns nonzero. `CONTROL_V1` remains
the control and `SHADOW_IMPACT_V1` is evaluated offline only. Numeric delta
without recognized provenance is unscoreable.

Evidence boundaries: this is hermetic certification evidence only. It does not
run providers, validate live discovery, repair the production planner handoff,
or constitute market-observation evidence. Trading is disabled and orders are
zero.
