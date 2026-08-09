# Phase 4: live multi-symbol universe validation

Phase 4 is merged at main commit `b62d017dc572fd4dfbef99afccfc3f01b4399840`.
Phase 4B mathematical footprint validation is prepared separately on
`feat/phase-4b-impact-shadow`; see [`../phase4b/README.md`](../phase4b/README.md)
and the [Monday runbook](../phase4b/MONDAY_RUNBOOK.md). Phase 5 has not started.

Phase 4 live multi-symbol validation is complete on implementation head
`88763d451bf7b07033eaec999d106879ecfaa285`; PR #4 is ready for owner review
and merge authorization. The final extended run was
`9c95f8f0-ae8f-4194-9152-0c33552bd170`.

The final run selected 92 call contracts across 12 symbols, acknowledged 92
TRADE and 92 QUOTE subscriptions, processed 24,255/24,255 trades, produced
30,009 synchronized inputs and 4,125 persisted decisions. Replay produced
4,125 decisions with field-by-field equality, and cross-symbol mismatches were
zero. It formed 1,799 sweep clusters and no qualifying sweeps. Zero qualifying
sweeps is an observed market result, not a pipeline failure: synchronization,
gate evaluation, persistence, and replay all completed successfully, while no
cluster satisfied every authoritative threshold. Trading remained disabled and
orders remained 0/0.

The earlier run
`ee806be1-c9d3-4917-b0a6-8e38054cf04d` remains historical evidence only. It
processed 31,596/31,596 accepted trades and formed 221 clusters, but produced
zero synchronized inputs because its smoke path requested AAPL while only BAC,
NFLX, and NVDA were selected. OI was incomplete but was not the primary
synchronization failure; replay of zero decisions was vacuous.
The bounded pilot symbols are defined in
`institutional_signal_engine.universe.PILOT_SYMBOLS`. A symbol is included only
when market capitalization is at least $10 billion, average daily dollar volume
is at least $50 million, listed options are confirmed, and average options volume
is at least 2,000 contracts. Missing data fails closed with explicit reasons.

For each included symbol, the selector sorts future expirations, requests strikes,
constructs call contracts, applies quote/open-interest/liquidity and 20% moneyness
filters, and selects the nearest expiration with qualifying contracts. The
pre-enrichment cap is 100 contracts per symbol and the total enrichment bound is
2,000 candidates. Plans are
deduplicated, sorted, capped at 15,000 contracts before connection, and rendered
as individual Standard `STREAM` requests. `STREAM_BULK` is not used.

Equity, benchmark, sector, option-flow, and sweep state are synchronized
independently per selected root; AAPL is not required unless selected. Missing
state is reported per symbol. Observation quote eligibility uses the separately
versioned `phase4-observation-spread-v1` policy (absolute spread <= $0.05 and
proportional spread <= 20%, with valid fresh positive-size quotes); this policy
does not alter production signal eligibility and must not be inherited by Phase 5.
The exact current production spread formula remains `ask - bid`, with
proportional spread `(ask - bid) / bid`; enrichment diagnostics preserve both
thresholds and rejection buckets. Rejected unacknowledged market-data events
are retained only as bounded run-scoped aggregates with an overflow counter and
never enter strategy state.

The ThetaData discovery client is retained only as a future cross-validation
source and is not a runtime dependency. The 2026-08-06 launcher-restarted
diagnostic used Terminal build `20260804:bdd51ae`: MDDS returned `200` with
`CONNECTED`; contract-list returned sanitized HTTP `472` JSON, and expiration
and strike discovery returned sanitized HTTP `500` HTML. Debug evidence showed
the Terminal's gRPC bridge could not load its zstd JNI library under the
launcher `/tmp` restrictions. Alpaca remains the primary catalog and the
expiration-plus-strike workflow is fixture-tested only.

No additional Phase 4 observation is required. The next action is owner review
and merge authorization for PR #4. Phase 5 begins only after the owner
explicitly authorizes and completes that merge. ThetaData REST discovery and
dated OI remain optional future cross-validation sources; they were not retried
for closure and are not a Phase 4 observation blocker.
