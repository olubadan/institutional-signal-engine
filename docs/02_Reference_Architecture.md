# Reference Architecture

This is the approved conceptual boundary model. It does not claim that any
component is implemented.

## Dependency rule

Dependencies point inward. Domain and signal policy do not import provider SDKs,
databases, caches, web frameworks, or transport objects. Adapters implement ports
owned by the application boundary.

## Planned boundaries

| Boundary | Responsibility | Excluded responsibility |
| --- | --- | --- |
| Configuration | Validate, redact, snapshot, and version non-secret configuration; obtain secrets through an injected source | Provider behavior or signal decisions |
| Equity data port | Define required equity events independently of Alpaca | Alpaca wire models outside its adapter |
| Options data port | Define required options-flow events independently of the selected vendor | Vendor fields outside its adapter |
| Symbol universe | Maintain canonical tradable symbols and sector relationships | Signal scoring or order submission |
| Normalization | Produce versioned canonical events with provenance | Gate policy |
| Time synchronization | Align streams under explicit ordering and lateness policy | Provider connections |
| Signal state | Maintain deterministic per-symbol state and compute the formal indicators | Network or persistence I/O |
| S/F/R/E gates | Evaluate validity, freshness, capacity, and executability with reason codes | Order submission |
| Ranking | Apply a total deterministic lexicographic ordering | Randomness or implicit wall-clock reads |
| Execution port | Express paper-order intent, acknowledgements, fills, and rejections | Signal computation |
| Position management | Apply approved sizing and entry/exit policy | Live-trading enablement |
| Persistence port | Record events, decisions, orders, fills, health, performance, and lineage | Business-policy mutation |
| Observability | Emit structured secret-free logs, metrics, health, and readiness | Authoritative business state |
| Replay | Feed recorded canonical events through the production decision path deterministically | Replay-only signal rules |

## Conceptual flow

```text
provider adapters -> normalization -> synchronization -> orchestration
                                                        |-> signal state
                                                        |-> S/F/R/E gates
                                                        |-> ranking
                                                        |-> paper execution port
                                                        |-> persistence port
                                                        `-> observability port
```

Configuration supplies a versioned snapshot to orchestration. Every decision
references the canonical inputs, configuration version, and engine version that
produced it.

## Architectural invariants

- The formal model in `03_Formal_Mathematical_Model.md` is authoritative.
- A candidate cannot fire unless the required `S`, `F`, and `R` gates pass, so
  `E = 1`.
- An empty candidate set produces the unique deterministic no-action state.
- Trading defaults disabled and only a verified Alpaca paper endpoint may receive
  orders.
- Redis, if adopted, is transient and not the authoritative audit record.
- PostgreSQL, if adopted as planned, retains decision lineage and durable history.
