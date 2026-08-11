# ADR: RTH Orchestration Recovery

**Status:** ACCEPTED
**Date:** 2026-08-11
**Branch:** feat/rth-orchestration-recovery
**Base commit:** 4a3afe8dd70fecc1b629ff7ef5614a652a572529

## Observed Current Architecture

The Phase 4 production runner (`phase4_live_smoke.py::run()`) is a single 800-line
async function that executes every stage — configuration, discovery, enrichment,
coverage planning, subscription construction, provider acknowledgement, event
intake, persistence, finalization, and exit — as one linear sequence. There is
no concept of a planner epoch, no mid-session reevaluation, and no incremental
subscription management.

The inner signal runner (`live_smoke.py::run()`) accepts a `CompositionInterfaces`
dependency-injection port, but the outer Phase 4 runner constructs every
dependency directly. The inner runner is the only path through which provider
events reach the pipeline, and it receives a fixed contract set at startup.

The certification harness (`phase4b_certify.py`) creates synthetic planner
epochs from a scenario definition and runs them alongside — not through — the
actual composition boundary. The planner records and composition records are
produced independently in the same executor. Invariants that assert "dynamic
admission" or "exact planner epoch consumed" are checking whether the scenario
definition declared different epochs, not whether the production runner
performed epoch transitions.

The provider (`ThetaDataOptionsProvider`) supports per-connection subscription
payloads, request/acknowledgement correlation, connection-generation tracking,
and a single-contract unsubscribe payload. It does not support incremental
mid-session subscription addition or removal without a full reconnect.

## Failure Mechanism

1. **Frozen universe.** Discovery, enrichment, and coverage planning execute
   once at startup. The selected contract set is passed to the inner runner and
   never changes. Candidates that become eligible after 09:30 are permanently
   excluded. Candidates that cease to be eligible are never removed.

2. **No epoch concept.** There is no `PlannerEpoch` type, no sequence number,
   no content hash, no transition record, and no replayable epoch history. The
   runner cannot express "the universe changed from this set to that set."

3. **Competing plan authorities.** The `--prepared-plan` flag creates a
   secondary plan path that validates against the internal plan but exists as
   a parallel authority. If both paths are available, there is no single source
   of truth for what the active plan is.

4. **Provider lacks mid-session subscription control.** The provider builds all
   subscription payloads at connection time. Adding a contract mid-session
   requires a full disconnect/reconnect cycle.

5. **Certification does not exercise the production composition.** The
   certification harness's planner epochs are synthetic — they don't drive the
   actual `live_smoke.run()` composition. The invariants that reference epoch
   properties are proxy checks, not direct verification.

## Selected Decision

**B. REPLACE WITH A THIN ORCHESTRATION SHELL**

A new `OrchestrationShell` class will own the session lifecycle and delegate to
existing components. It will introduce a `PlannerEpoch` data structure, a
deterministic reevaluation timer, and an incremental subscription adapter
wrapping the existing provider.

## Rejected Alternative

**A. REPAIR EXISTING OUTER RUNNER**

Rejected because:
- The `phase4_live_smoke.py::run()` function is 800 lines of procedural code
  with intertwined discovery, enrichment, planning, subscription, intake,
  reporting, and finalization logic.
- Adding epoch management, incremental subscription, reconnect-with-epoch-state,
  and deterministic reevaluation to this function would require threading new
  state through every stage and would be extremely error-prone.
- The function does not accept a dependency-injection port, so testing epoch
  transitions would require monkeypatching.
- The effort to repair would likely exceed the effort to build a clean shell
  that composes existing, tested components.

## Production Components Retained

| Component | Fate |
|-----------|------|
| `Settings` / `config.py` | Unchanged |
| `AlpacaEquitiesProvider` | Unchanged |
| `AlpacaOptionsContractProvider` | Unchanged |
| `AlpacaOptionSnapshotProvider` | Unchanged |
| `ThetaDataOpenInterestProvider` | Unchanged |
| `ThetaDataOptionsProvider` | Wrapped by `DynamicSubscriptionAdapter` |
| `build_coverage_plan()` / `impact_coverage.py` | Unchanged — called per epoch |
| `build_pilot_coverage_candidates()` | Unchanged |
| `finalize_liquidity()` / `liquidity.py` | Unchanged |
| `SignalPipeline` / `pipeline.py` | Unchanged |
| `ShadowImpactEngine` / `impact.py` | Unchanged — SHADOW_IMPACT_V1 preserved |
| `CONTROL_V1` decision logic | Unchanged |
| `InMemoryRepository` / `PostgresRepository` | Unchanged |
| `AsyncAuditWriter` | Unchanged |
| `replay()` / `replay.py` | Extended for multi-epoch replay |
| `UniverseManifest` | Extended with epoch field |
| `RunUniverseFinalization` | Extended with epoch history |
| `live_smoke.py::run()` inner runner | Called per session (once), not per epoch |
| `phase4_live_smoke.py` outer runner | **Replaced** by `OrchestrationShell` |
| `phase4b_certify.py` | **Repaired** to drive actual composition |

## Interfaces Introduced

### `PlannerEpoch`

```python
@dataclass(frozen=True)
class PlannerEpoch:
    epoch_id: str  # UUID
    sequence: int  # monotonically increasing, starts at 1
    effective_at: datetime
    candidate_population_version: str
    selected_contracts: tuple[ThetaContract, ...]
    trade_subscriptions: tuple[ThetaContract, ...]
    quote_subscriptions: tuple[ThetaContract, ...]
    additions: tuple[ThetaContract, ...]  # from previous epoch
    removals: tuple[ThetaContract, ...]  # from previous epoch
    planner_version: str
    content_hash: str  # SHA-256 of canonical JSON
    provenance: str  # "production-planner-v1"
    lifecycle: str  # "pending" | "active" | "superseded" | "finalized"
```

### `DynamicSubscriptionAdapter` (Protocol)

Wraps `ThetaDataOptionsProvider` to support:
- `current_subscriptions() -> tuple[ThetaContract, ...]`
- `add_subscriptions(contracts: tuple[ThetaContract, ...]) -> tuple[int, ...]`
- `remove_subscriptions(contracts: tuple[ThetaContract, ...]) -> tuple[int, ...]`
- `acknowledge(timeout: float) -> SubscriptionAcknowledgement`
- `restore_epoch(epoch: PlannerEpoch) -> None`
- Events from unacknowledged contracts rejected with diagnostics

### `OrchestrationShell`

```python
class OrchestrationShell:
    def __init__(self, *, settings, clock, discovery, enrichment, planner,
                 subscription_adapter, event_streams, persistence, lifecycle):
        ...

    async def run(self) -> SessionResult:
        # 1. Pre-open initialization
        # 2. Auth and provider readiness
        # 3. Initial discovery → enrichment → Epoch 1
        # 4. Epoch 1 persistence and activation
        # 5. Paired subscription request + acknowledgement
        # 6. Event intake loop:
        #    a. Process events
        #    b. Scheduled RTH reevaluation → epoch diff → activation
        #    c. Disconnect detection → reconnect → idempotent restoration
        # 7. Intake stop at boundary
        # 8. Drain → finalize → exit
```

### Composition root for testing

The same `OrchestrationShell` is used by the production command and by tests.
Tests inject deterministic ports (`clock`, `discovery`, `enrichment`, `planner`,
`subscription_adapter`, `event_streams`, `persistence`, `lifecycle`).

## State Authority

Exactly one `OrchestrationShell` instance owns the session lifecycle. The
`active_epoch` property is the single authority for the current epoch. The
`PlannerEpoch.sequence` is monotonically increasing. The planner produces epochs;
the shell activates them; the subscription adapter acknowledges them; the
pipeline consumes events against the active epoch.

The `--prepared-plan` path is removed. The `--plan-output` path (write-only
diagnostic) is retained for offline inspection but has no authority.

## Migration Boundary

- New file: `src/institutional_signal_engine/orchestration.py`
- New file: `src/institutional_signal_engine/dynamic_subscriptions.py`
- Modified: `src/institutional_signal_engine/replay.py` (multi-epoch support)
- Modified: `src/institutional_signal_engine/universe.py` (PlannerEpoch additions)
- Modified: `src/institutional_signal_engine/phase4b_certify.py` (drive actual composition)
- Modified: `src/institutional_signal_engine/phase4_live_smoke.py` (delegate to shell or become entry point wrapper)
- New tests: `tests/test_orchestration.py`

## Rollback Boundary

- `CONTROL_V1` model: unchanged
- `SHADOW_IMPACT_V1` model: unchanged
- `SignalPipeline`: unchanged
- Provider adapters: wrapped, not rewritten
- Coverage planner: unchanged
- Liquidity assessment: unchanged
- Persistence: unchanged
- Replay: backward-compatible extension

Rollback is possible by restoring `phase4_live_smoke.py` and removing the new
orchestration files. No model, pipeline, or provider changes need reversal.

## Testing Strategy

1. **Hermetic unit tests** for `PlannerEpoch` invariants (immutability, hash, sequence)
2. **Hermetic unit tests** for `DynamicSubscriptionAdapter` (add, remove, ack, timeout, reject)
3. **Hermetic integration tests** for `OrchestrationShell` with deterministic ports
   covering the 35 scenarios listed in Phase 6
4. **Existing suite** must continue to pass
5. **Certification harness** repaired to drive the actual `OrchestrationShell` composition
   with deterministic ports

All tests use virtual time. No real market hours, no live providers.

## Remaining Unknowns

1. **ThetaData provider behavior on mid-session STREAM add.** The ThetaData
   Terminal's `STREAM` message with `add: true` should work mid-connection, but
   this has not been verified against a live Terminal. The adapter will send
   the payload and treat a `SUBSCRIBED` response as acknowledgement. If the
   Terminal requires a full reconnect for subscription changes, the adapter's
   reconnect path handles this.

2. **Optimal reevaluation interval.** The default will be 5 minutes,
   configurable. The optimal interval depends on how quickly option liquidity
   profiles change during RTH — this requires live observation to determine.

3. **Capacity-bound epoch transitions.** When capacity is binding, the planner
   may produce the same selected set even when the candidate population changes.
   The no-op detection (content hash comparison) correctly suppresses duplicate
   epochs in this case.

4. **Reconnect during epoch transition.** If the provider disconnects while an
   epoch's subscriptions are being acknowledged, the shell reconnects and
   restores the current (not the pending) epoch, then retries the transition.
