# Glossary

| Term | Meaning |
| --- | --- |
| Alpaca paper | Alpaca's simulated trading environment; the only authorized order destination. |
| Canonical event | Provider-neutral, versioned normalized event carrying source and timing provenance. |
| Candidate set `C(t)` | Symbols for which `E(i,t) = 1` at time `t`. |
| Configuration version | Stable identifier for the validated non-secret configuration snapshot used by a decision. |
| `E(i,t)` | Executability gate: `S(i,t) ∧ F(i,t) ∧ R(i,t)`. |
| Episode | Continuous signal-valid interval beginning at the first `S(i,·) = 1` used to define `t₀(i)`. |
| `F(i,t)` | Freshness gate combining decay, options-flow persistence, and equity direction. |
| Fire | Indicator that the candidate set is nonempty. |
| Institutional signal | A symbol state satisfying the specified liquidity, options, equity, market, sector, freshness, room, and executability conditions. |
| Kill switch | Operational control that prevents new order submission and fails closed. |
| Open interest (`OI`) | Outstanding options contracts used in the option-volume/open-interest ratio. |
| Port | Provider- or infrastructure-neutral interface owned by an inner application boundary. |
| Provenance | Source identity and source/receipt/normalization timing attached to an input. |
| `R(i,t)` | Room/capacity gate covering resistance distance, liquidity, spread, and concurrent-position capacity. |
| Replay | Deterministic reprocessing of recorded canonical events through the production decision path. |
| `RVOL` | Relative volume used as a lexicographic ranking key. |
| `S(i,t)` | Signal-validity conjunction of liquidity, options, equity, market, and sector indicators. |
| Stale data | Data older than the configured freshness limit for its source or decision use. |
| Vast.ai instance | Persistent approved Ubuntu development/runtime host; rental requires prior owner selection. |
