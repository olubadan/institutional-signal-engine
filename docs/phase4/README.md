# Phase 4: live multi-symbol universe validation

Phase 4 preparation is fixture-tested only. No regular-session live validation
was run during initialization.

The bounded pilot symbols are defined in
`institutional_signal_engine.universe.PILOT_SYMBOLS`. A symbol is included only
when market capitalization is at least $10 billion, average daily dollar volume
is at least $50 million, listed options are confirmed, and average options volume
is at least 2,000 contracts. Missing data fails closed with explicit reasons.

For each included symbol, the selector sorts future expirations, requests strikes,
constructs call contracts, applies quote/open-interest/liquidity and 20% moneyness
filters, and selects the nearest expiration with qualifying contracts. Plans are
deduplicated, sorted, capped at 15,000 contracts before connection, and rendered
as individual Standard `STREAM` requests. `STREAM_BULK` is not used.

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
