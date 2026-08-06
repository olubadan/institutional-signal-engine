# Phase 4: live multi-symbol universe validation

Phase 4 preparation and the first regular-session observation are complete;
the next regular-session validation must use the corrected multi-symbol head.
The historical run `ee806be1-c9d3-4917-b0a6-8e38054cf04d` processed
31,596/31,596 accepted trades and formed 221 clusters, but produced zero
synchronized inputs because its smoke path requested AAPL while only BAC,
NFLX, and NVDA were selected. OI was incomplete but was not the primary
synchronization failure; no live strategy decision or complete signal was
demonstrated, and replay of zero decisions was vacuous.

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

Next-session command, only after 09:30 America/New_York and with trading still
disabled:

```sh
cd /opt/institutional-signal-engine
uv run python -m institutional_signal_engine.phase4_live_smoke --seconds 60
```

This command is reserved for the next regular-session validation and was not run
during Phase 4 initialization.
