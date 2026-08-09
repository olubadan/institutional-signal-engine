# Phase 4B — Mathematical footprint validation

Phase 4B is a research-only shadow validation branch from merged Phase 4
baseline `b62d017dc572fd4dfbef99afccfc3f01b4399840`. Phase 4 is merged;
Phase 5 has not started.

## Models

`SHADOW_IMPACT_V1` evaluates the existing normalized option-trade and sweep
evidence in the shared pipeline. It uses the exact-contract, inclusive,
non-sliding one-second event-time cluster already enforced by `SweepEngine`.
For each eligible trade it calculates premium `p*N*100`, signed and gross
delta-equivalent demand, and directional coherence. Delta is accepted only
with explicit provider/calculation provenance; otherwise the record carries
`IMPACT_DELTA_UNAVAILABLE` and is not force-classified.

The frozen five-minute baseline is derived from completed, split-adjusted
Alpaca historical bars, excluding the current session and requiring at least
20 completed sessions. It stores expected same-time volume, return volatility,
effective date, sample size, adjustment metadata, and provenance. Missing or
insufficient history produces `IMPACT_BASELINE_INSUFFICIENT`.

The model persists `Y=1` as `impact_coefficient` with source
`RESEARCH_ASSUMPTION_V1`, and `pi=0.0025` as `target_move` with source
`OWNER_SELECTED_TARGET_UNDERLYING_MOVE`. Expected volatility and expected
volume remain separate frozen baseline values. It calculates
`I=Y*sigma*sqrt(abs(Q_signed_delta)/V)` and `Z=I/pi`. Cluster
qualification requires `Z>=1`, coherence `>=0.65`, three valid versioned
exchanges, ask percentage `>=65`, UNKNOWN premium percentage `<=50`, and the
one-second window. Session state uses fresh qualifying clusters and 30-minute
exponential decay of signed delta demand; it does not add individual Z scores.

The existing fixed-dollar detector remains `CONTROL_V1` and is unchanged. It
is a deterministic post-session replay control, not a second live engine.
Every comparison is classified as CONTROL_PASS_SHADOW_PASS,
CONTROL_PASS_SHADOW_FAIL, CONTROL_FAIL_SHADOW_PASS, or
CONTROL_FAIL_SHADOW_FAIL.

## Coverage

Conditional coverage uses `U_M`, `U_X`, and `U_R` and keeps unresolved upper
bounds in the theoretical required set. The planner is now connected to the
bounded 20-symbol pilot runner: every quote-liquid pilot candidate receives a
versioned coverage decision, and only capacity-selected members of
`U*=U_M∪U_R` reach the paired TRADE/QUOTE subscription plan. TRADE capacity
is 15,000 and QUOTE capacity is 10,000, so paired observation is capped at
10,000; they are enforced independently. When capacity binds the status is
`CAPACITY_CONSTRAINED_COVERAGE`. The deterministic priority factors are
bounded impact capability, existing Phase 4 liquidity rank quality, and
evidence completeness, with versions and exclusion evidence persisted. No
historical maximum is treated as a hard future bound.

## Evidence and policy

The shadow result is persisted through the same repository/writer used by the
shared event and sweep path. Replay uses the persisted sweep audits, accepted
event payloads, frozen baselines, and model configuration. It does not alter
CONTROL_V1 decisions. `SHADOW_IMPACT_V1` is research-only and must not be
promoted to production or inherited by Phase 5 automatically.

No genuine SHADOW_IMPACT_V1 footprint has yet been validated. Weekend work is
fixture-only; no live provider session is authorized here. The bounded
experiment requires at least five complete regular sessions and 30 shadow
footprints before reporting one of `RETAIN CONTROL`, `REVISE IMPACT MODEL`,
`PROMOTE IMPACT MODEL`, or `NO FOOTPRINT DEMONSTRATED`.

## Weekend benchmark and Monday execution mode

Delta values are usable only with the versioned allowlist
`impact-delta-provenance-v1`: `ALPACA_PROVIDER_DELTA_V1`,
`THETADATA_PROVIDER_DELTA_V1`, or `DETERMINISTIC_OPTION_DELTA_V1`. Missing
delta records `IMPACT_DELTA_UNAVAILABLE`; numeric delta without recognized
provenance records `IMPACT_DELTA_PROVENANCE_UNAVAILABLE`. Raw values and
provenance state are retained for replay.

The deterministic benchmark command is
`uv run python scripts/benchmark_phase4b_impact.py`. It used 256 clusters,
three constituents per cluster, three warmup samples, and 15 measured
samples on local hermetic Python 3.12. Shared/control feature processing was
6.746 ms p50 (7.874 ms p95, 8.207 ms max), combined processing was 18.255 ms
p50 (21.425 ms p95, 22.881 ms max), and incremental shadow scoring was
11.425 ms p50 (15.033 ms p95, 16.174 ms max). Measured p50 overhead was
169.36%, above the 5% budget. The approved fallback therefore applies:
shared feature and sweep evidence remain the single persisted live path, and
live `SHADOW_IMPACT_V1` scoring is disabled pending optimization; shadow and
`CONTROL_V1` are calculated only in deterministic post-session replay.

The pilot coverage manifest distinguishes the Alpaca-discovered 20-symbol
candidate population, theoretical `U*`, the capacity-selected observation
subset, and the provider-acknowledged subset. In the current implementation,
the prospective `Q̄_j^Δ` upper bound, delta provenance, and exchange
participation are unavailable at planning time, so those candidates remain
`U_R`; this is evidence, not a defect. Market-wide cataloging, market-wide
`U*`, a defensible universal upper-bound method, and a market-wide zero-miss
guarantee are not implemented.
