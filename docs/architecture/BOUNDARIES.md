# Architecture Boundaries

This document defines dependency direction; it does not claim that the components
are implemented.

## Dependency rule

Dependencies point inward. Domain and signal policy cannot import infrastructure,
provider SDKs, databases, caches, web frameworks, or transport-specific objects.
Adapters implement typed ports owned by the application boundary.

## Planned components

| Boundary | Responsibility | Must not own |
| --- | --- | --- |
| Configuration | Load, validate, redact, and version non-secret configuration; obtain secrets through an injected source | Provider logic or business decisions |
| Equity market-data port | Express required equity events independently of Alpaca | Alpaca wire models outside its adapter |
| Options-data port | Express required options-flow events independently of a vendor | Vendor-specific fields outside its adapter |
| Symbol universe | Maintain canonical tradable symbols and sector relationships | Signal scoring or order submission |
| Normalization | Convert provider events into versioned canonical events with provenance | Gate policy |
| Time synchronization | Order and align streams under explicit lateness and clock policies | Vendor connections |
| Signal state | Maintain deterministic per-symbol inputs and compute approved indicators | Persistence or network I/O |
| Gates S/F/R/E | Evaluate validity, freshness, capacity, and executability with reason codes | Order submission |
| Ranking | Produce a total, deterministic lexicographic ordering | Hidden randomness or wall-clock reads |
| Execution port | Express paper-order intent, acknowledgements, fills, and rejections | Signal computation |
| Position management | Apply sizing and approved entry/exit policies | Live-trading enablement |
| Persistence port | Append events, decisions, orders, fills, health, and performance with lineage | Business-policy mutation |
| Observability | Emit structured logs, metrics, health, and readiness without secrets | Source-of-truth business state |
| Replay | Feed recorded canonical events through the same application path deterministically | Special-case signal rules |

## Proposed dependency flow

```text
provider adapters -> normalization -> synchronization -> application orchestration
                                                        |-> signal domain
                                                        |-> gates and ranking
                                                        |-> paper execution port
                                                        |-> persistence port
                                                        `-> observability port
```

Cross-cutting configuration supplies a versioned snapshot to orchestration. Every
decision must reference the exact canonical inputs, configuration version, and
engine version that produced it.

## Safety invariants

- Live trading is not an implementation target.
- Trading defaults to disabled and execution fails closed.
- Only a verified Alpaca paper endpoint may receive orders.
- A candidate cannot fire unless all required S/F/R/E gates pass.
- No candidate produces a deterministic, recorded no-action decision.
- Identical ordered input events plus identical configuration and engine versions
  produce identical decisions.

The formal indicator equations and complete lexicographic keys remain blocked
until the authoritative mathematical specification is added to the repository.
