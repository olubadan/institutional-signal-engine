# Current State

- **Project mission:** Build a production-oriented, deterministic, real-time
  institutional signal engine using Alpaca equity data and paper execution,
  a replaceable external options-data provider, synchronized normalized events,
  formal S/F/R/E gates, deterministic ranking, traceable persistence,
  observability, replay, and reproducible Vast.ai operation.
- **Current phase:** Phase 4 merged; Phase 4B causal-journal recovery is active
  on stacked draft PR #6; Phase 5 has not started
- **Completed phases:** Phase 0 — Ground the Current State; Phase 1 — canonical
  documentation baseline; Phase 2 — reproducible Vast environment verified with
  documented Ubuntu 22.04/finite-duration exception and merged in PR #2
- **Active branch:** `feat/rth-orchestration-recovery`
- **Phase 4 merge:** PR #4 is merged into `main` at
  `b62d017dc572fd4dfbef99afccfc3f01b4399840`; resulting-main CI run
  `31209185798` passed.
- **Pull-request state:** Recovery draft PR #6 is open from
  `feat/rth-orchestration-recovery`, stacked on open draft PR #5; Phase 4 PR #4
  is closed and merged.
- **Current architecture:** Phase 3 now includes typed stateful indicator
  primitives, fail-closed missing-data reasons, continuous event processing,
  correlated Standard exact-contract requests, dynamic-universe selection ports,
  typed PostgreSQL event replay, separate candidate counters, and hermetic CI.
  Execution remains absent.
- **Vast environment state:** Owner-created instance `46725769` is running and
  SSH-accessible. Runtime inspection measured Ubuntu 22.04, 19 visible vCPU,
  approximately 49.3 GiB RAM, and 130 GB allocated disk. Docker/Compose, Python
  3.12, `uv`, Git, GitHub CLI, PostgreSQL/Redis clients, build tools, protected
  configuration, local persistent dependency services, restart recovery,
  connectivity, and SSH reconnection are verified. Trading is disabled. Contract
  end is `20260901T103519Z`. Historical evidence: Vast CLI authentication succeeded
  without exposing account data. The owner-selected offer `40176329` / machine
  `138965` returned no exact marketplace record in two pre-charge queries at
  `20260803T183129Z` and `20260803T183142Z`, including one without default
  availability filters, and remained absent in the owner-authorized retry at
  `20260803T183559Z`. Newly selected offer `39005761` also returned no record in
  standard and no-default exact-ID searches at `20260803T184616Z` and
  `20260803T184627Z`. No rental, charge, provisioning, or substitution occurred.
- **Provider-integration state:** Fixture-testable Alpaca and ThetaData v3
  adapters implemented. Runtime loading is data-only; required provider fields are present in the protected
  runtime file; values were never displayed. Alpaca feed-name parsing is fixed and
  regression-tested. The official Theta Terminal runtime is active with both
  ports loopback-only. Options Standard exact-contract trade streaming is now
  implemented for the supplied AAPL contract. The final corrected
  2026-08-05 smoke received 60 option trades and 15,763 Alpaca events; the
  Terminal acknowledgement correlated successfully through `header.req_id`.
- **Test state:** Phase 1 structural checks remain passed. Phase 2 `make setup`
  passed twice; `make lint`, `make typecheck`, `make build`, and `make test` passed
  on Vast. Verification covered resources, tool versions, root-only environment
  files, trading-disabled state, branch, systemd/Docker enablement, container
  health, local connectivity, filesystem/PostgreSQL/Redis restart persistence,
  SSH key-only policy, reconnection, outbound HTTPS, and clean checkout. Phase 3
  checks pass: `uv sync`, Ruff, mypy, 27 pytest tests split across provider,
  reconnection, signal, persistence, and replay groups. ThetaData service/runtime
  startup and loopback binding passed. The six internal runtime variables were
  restored under explicit authority; dependency connectivity and filesystem,
  PostgreSQL, and Redis persistence survived restart. Regular-session live smoke
  passed with zero orders constructed or submitted.
- **Approved decisions:** Phase 1 is canonical documentation only; use branch
  `chore/engineering-baseline-bootstrap`; the 14-part owner model is authoritative;
  maintain the append-only Codex journal; GitHub remains authoritative; paper
  trading only; no merge or paid Vast provisioning without explicit approval;
  use owner-created instance `46725769` with the ADR-0001 Ubuntu 22.04 and finite-
  duration exception.
- **Pending approvals:** Owner review of the explicit specification and
  provider-acknowledgement blockers; private-repository branch protection
  remains pending.
- **Known blockers:** ThetaData MDDS reports CONNECTED and the supplied Standard
  exact-contract stream acknowledges successfully, but the documented AAPL
  contract-list, expirations, and strikes helpers return Jetty HTTP 500. The
  supplied contract is used for validation; dynamic live discovery remains
  unverified. The formal model does not define the eligible-trade set,
  ask-side classification, or five-day resistance/high formula; these three
  definitions are now owner-specified and implemented in SESSION-0010. GitHub branch
  protection/rulesets remain unavailable on the current private-repository plan.
  Latest run-scoped replay equality passed; the remaining provider blocker is
  dynamic discovery HTTP 500. Run-scoped deterministic replay succeeded for
  run eba46d34-4335-486d-9929-33863f44b331. PostgreSQL contained 15,381
  accepted events and 15,131 decisions. Replay produced 15,131 decisions
  with field-by-field equality. This proof applies to this corrected run under
  its recorded engine, configuration and condition-mapping versions; it does
  not claim that every historical run is replayable.
- **Next action:** Obtain final independent validation of the exact-head causal
  journal correction on draft PR #6. Phase 5 begins only after Phase 4B scope is
  complete and the owner explicitly approves it.

## Phase 4B weekend preparation — SESSION-0022

- `SHADOW_IMPACT_V1` is a research-only mathematical detector attached to the
  existing shared normalized-event, quote-classification, sweep, and audit
  pipeline. It does not run a second live engine and cannot alter `CONTROL_V1`.
- The implemented formulas use Decimal premium, signed/gross delta-equivalent
  demand, coherence, five-minute Alpaca historical baselines, separate
  `impact_coefficient=1` (`RESEARCH_ASSUMPTION_V1`) and
  `target_move=0.0025` (`OWNER_SELECTED_TARGET_UNDERLYING_MOVE`), one-second
  clusters, and 30-minute decayed session demand. Missing delta or numeric
  delta without recognized versioned provenance fail closed with explicit
  reasons and retain raw evidence.
- Conditional coverage uses `U_M`, `U_X`, and `U_R`, separate 15,000 TRADE and
  10,000 QUOTE capacities, versioned bounded priority factors, and persisted
  capacity exclusions. The planner is now connected to the bounded 20-symbol
  pilot runner; only capacity-selected U* members reach paired subscription.
  Pilot prospective delta-demand bounds remain unavailable, so the current
  pilot evidence is honestly classified as U_R rather than U_X. No universal
  zero-miss claim is made.
- Fixture verification: 156 hermetic tests pass, including shared-pipeline
  shadow persistence, Decimal boundary arithmetic, session decay/expiry,
  baseline provenance, conditional capacity, and async audit draining. Ruff
  format/lint and strict mypy pass. No live market/provider session was run.
- The deterministic benchmark used 256 clusters, three warmup samples, and 15
  measured samples. Shared/control processing was 6.746 ms p50 and combined
  processing was 18.255 ms p50, for 169.36% incremental p50 overhead. This
  exceeds the approved 5% budget, so the approved fallback is active: the
  shared feature vector and sweep evidence are persisted, while live shadow
  scoring is disabled pending optimization; shadow and CONTROL_V1 scoring are
  post-session replay work only.
- No genuine mathematical footprint has been validated yet. The existing
  Phase 4 observation remains unchanged: zero qualifying fixed-dollar sweeps
  was an observed market result, not a pipeline failure. Trading is disabled;
  orders remain `0/0`.
- RAID: `docs/governance/RAID.md`; Monday runbook:
  `docs/phase4b/MONDAY_RUNBOOK.md`; design:
  `docs/phase4b/README.md`.
- Published draft PR [#5](https://github.com/olubadan/institutional-signal-engine/pull/5)
  at the current branch tip. Exact-head push and pull-request CI both pass;
  the final run IDs are recorded on GitHub and in the closing PR evidence.

## Ignition harness correction — SESSION-0024

- The first Monday Phase 4B launch was fail-closed. The five reported failures
  were probe/composition mismatches: host `pg_isready` did not address the
  containerized application DSN, Alpaca ignition bypassed the typed provider,
  persistence was inferred rather than enqueued and drained, and the plan
  checks did not run the production discovery/enrichment/planner path.
- The correction uses the application `Settings` and `PostgresRepository`,
  `AlpacaEquitiesProvider.current_prices()`, the typed MDDS status adapter, and
  a reversible `AsyncAuditWriter` probe. The live runner now supports a
  prepare-only planner pass and exact prepared-plan validation before stream
  consumption. Trading remains disabled and orders remain `0/0`.
- Exact-head CI passed on correction head `a9a7bace1c991776eafeaa65b85f3036dcaef21a`;
  ignition passed on the VM and produced a protected 40-contract plan
  (`U_M=0`, `U_X=0`, `U_R=40`, `U*=40`). The recovery command then failed
  before WebSocket connection because the repeated discovery/enrichment pass
  did not reproduce the prepared plan (`911` versus `912` completed quote
  results), yielding the runner's `prepared_plan_mismatch` failure point.
- Run `0950e1c9-0e67-42b2-9f55-98d545650e52` is the ignition/preflight run,
  not a live observation: its scope has zero events, decisions, quotes,
  clusters, and finalization. No live Phase 4B session counts toward the
  five-session/30-footprint requirement. Trading remains disabled and orders
  remain `0/0`; Tuesday is blocked pending an authoritative prepared-plan
  handoff correction.

## Final engineering review — 20260805

- Implementation head `b991f95091fee78397adfadf4125dda4671f2094` is pushed on
  `feat/signal-only-vertical-slice`. The review found and corrected one
  auditability defect: a single quote may classify multiple trades, so each
  distinct trade now receives its own deterministic consumed-quote audit row;
  the unique quote counter remains incremented once. Regression coverage was
  added.
- Quote counters are defined as follows: `quotes_received` counts every
  parsed quote message; `current_state_overwrites` counts slot replacement
  operations and is intentionally overlapping; `quotes_consumed` counts
  unique quote identities audited at least once; and
  `quotes_pending_at_shutdown` counts unique latest-slot identities not
  consumed. The lifecycle invariant is
  `quotes_consumed + quotes_pending_at_shutdown <= quotes_received`.
- Final checks passed locally: Ruff format, Ruff lint, strict mypy, and 35
  hermetic tests. The test harness rejects AF_INET/AF_INET6 sockets for all
  tests; no tests are marked for live integration. GitHub Actions passed on
  the final head at
  `https://github.com/olubadan/institutional-signal-engine/actions/runs/31028561845`.
- Review evidence confirms the prior run processed and retained all accepted
  trades (`115/115`), persisted 115 trade events, and replayed persisted
  decisions with field-by-field equality. Trading stayed disabled and orders
  constructed/submitted remained `0/0`.
- This earlier review snapshot was superseded by the indicator, boundary, and
  owner-authorized sweep closure records below. Dynamic ThetaData discovery
  remains fixture-tested but not live-verified because the documented
  discovery helpers return HTTP 500; the supplied exact-contract stream
  remains live-verified.

## Indicator and boundary closure pass — 20260805

- Implementation commit `36a8626` wires a typed Alpaca historical bootstrap
  to split-adjusted completed daily/minute bars, previous-close deltas,
  session VWAP, same-minute RVOL, timestamp-aligned SPY/sector relative
  strength, and cached five/twenty/252-session resistance with namespaced
  provenance and source dates. Missing baselines fail closed with persisted
  reason codes; adapter placeholders cannot reach Alpaca decisions.
- The pipeline now persists indicator provenance in decisions, recomputes
  resistance distance against frozen session ladders, and resets
  session-scoped state idempotently at ET regular-session open/close. The
  live smoke timer invokes boundary handling even without incoming events.
- Live run `4faad966-f4fb-44c4-8f2f-58ec440c59c6` succeeded during regular
  hours: historical bootstrap success for AAPL/SPY/XLK; Alpaca 11,440 events;
  ThetaData 16 option events; authentication and exact-contract
  acknowledgement succeeded; trades received/processed `59/59`; stale,
  late, duplicate, and out-of-order `0/0/0/0`; evaluations triggered/skipped
  `3/36`; trigger reasons included `session_boundary_open`; unknown
  conditions `6`; candidate counters `1/0/0/0`; orders `0/0`.
- Run-scoped PostgreSQL contained 59 events, 61 consumed-quote audit records,
  and 3 decisions. Replay produced 3 decisions with field-by-field equality.
  Internal timing remained low: Alpaca quote queue-wait p50/p95 `0.053/0.122`
  ms and processing p50/p95 `0.036/0.090` ms; Theta option trade queue-wait
  p50/p95 `0.049/0.069` ms and processing p50/p95 `0.410/0.954` ms. No
  persistence soft/hard limit activity occurred.
- This earlier note was superseded by the owner-authorized sweep closure
  below. ThetaData discovery HTTP 500 remains the separate external provider
  blocker; no discovery endpoint was retried.

## Throughput correction — 20260805

- Session-0011 implements O(1) latest-value quote slots, bounded event-time
  classification windows, immutable individual trade persistence, material-
  change-only evaluation, timer-driven freshness checks, asynchronous batched
  audit writes, explicit soft/hard backpressure, and deterministic consumed-
  quote replay.
- Final 60-second run ID `b9e0f47a-a16d-422f-b389-0386d66634ef`: 10,483 total
  events (`10,376` quotes and `115` trades); quotes received/current-state
  overwrites/consumed were `10,376/10,374/91`; pending-at-shutdown is reported
  separately by the corrected smoke path;
  trades received/processed `115/115`;
  Current-state overwrites are slot-replacement operations and intentionally
  overlap with consumed/pending quote identities; they are not a lifecycle
  bucket. The lifecycle invariant is `quotes_consumed +
  quotes_pending_at_shutdown <= quotes_received`.
  evaluations triggered/skipped `5/101`; stale/late/duplicate/out-of-order
  `0/0/0/0`; unknown conditions `5`; candidate counters `1/0/0/0`.
- Internal targets passed. Internal queue-wait p50/p95 was below 0.1/0.2 ms
  for Alpaca quotes and trades; processing p95 was below 0.1 ms for quotes and
  0.98 ms for trades. Total age remains reported separately by provider and
  event kind. Queue depth p50/p95/max was `1/4/11`; database-write latency
  p50/p95/max was `20.355/25.078/36.394 ms`; no soft or hard limit activity.
- PostgreSQL contained 115 trade events, 91 consumed quotes and 5 decisions.
  Replay consumed those exact inputs and produced 5 decisions with
  field-by-field equality. Quote roles included `BOTH` and `CURRENT_STATE`.
- PR #3 remains draft and unmerged. The external ThetaData discovery HTTP 500
  blocker remains unchanged; no discovery endpoint was retried.

## Closure-pass publication — 20260805T1615Z

- Implementation head `07587abfe348a95b7625dca92f999e25b4068361`, documentation
  closure commit `12d435cc78a5fb17870ef67672af129823177b5a`, and journal record
  correction commit `0421fca66150ca27ba962d8be5594353d3a5de4b` are pushed to the
  feature branch. GitHub Actions CI passed at
  `https://github.com/olubadan/institutional-signal-engine/actions/runs/31023522485`.
- Final 60-second run ID `14a4ab65-3c3b-449a-aa49-d6eaf30dd352`: Alpaca 15,845
  events; ThetaData 38 option events; acknowledgements and authentication
  succeeded; synchronized evaluations 15,058; stale 0, late 0, duplicate
  345, out-of-order 153, unknown conditions 8; counters 1/0/0/0; orders 0/0.
- Run-scoped PostgreSQL contained 15,385 accepted events and 15,058 decisions.
  Replay produced 15,058 decisions with field-by-field equality under mapping
  `thetadata-trade-conditions-v3-20260805`.
- Timing distributions are provider/event-kind specific and preserve raw event
  age at receipt and processing duration. VM clock synchronization was yes,
  system timezone UTC, Terminal status CONNECTED, and no reconnect/buffering
  indication appeared in the inspected Terminal log window. Multi-second age
  measurements remain reported without clamping; plan-level throttling was not
  established from official documentation or support confirmation.

## Work Package 1 correction — SESSION-0026

- The declarative certificate at `639a071` was replaced by an executable
  deterministic scenario executor. It consumes virtual-time actions in order,
  invokes the production signal composition boundary with deterministic
  provider/clock/persistence ports, runs real coverage planning across epochs,
  executes normalized events, impact evaluation, persistence, replay, and
  lifecycle observations, and derives every invariant from trace/state.
- The signal runner gained an optional deterministic composition interface;
  default production construction is unchanged. The Phase 4 outer runner was
  not repaired. Its missing composition interface is recorded as an observed
  failed invariant, so the certificate remains honestly `FAIL`.
- JSON Schema validation uses `jsonschema` Draft 2020-12 with format checking;
  negative schema and anti-cheating tests cover mutation, duplicate events,
  capacities, constants, nested fields, UUID/SHA formats, and extra fields.
- Runtime output is external-only at `/tmp/phase4b-certification/CERTIFICATE.json`;
  the committed `PHASE4B_CERT_V1.example.json` is explicitly not evidence.
  The correction commit is `4366407ad62ace1b24989493e22780935370cafd`; the
  runtime certificate identifies the exact clean checked-out head at execution.
  Trading remains disabled; orders are `0/0`; no providers ran; Phase 5 remains
  unopened.

## Owner-authorized sweep closure — 20260805

- Implemented `NEW_QUALIFYING_SWEEP` in `sweeps.py` exactly to the owner
  definition: call-only exact contract identity, fixed inclusive 1,000 ms
  event-time clusters, three valid exchanges, Decimal premium and directional
  thresholds, separate session thresholds, one-shot qualification transitions,
  revocation/requalification, 30-minute freshness expiry, and deterministic
  correction/cancellation handling.
- PostgreSQL persists stable cluster IDs, constituents, threshold results,
  state transitions, session totals, freshness timestamps, mapping/engine
  versions, and provenance. Closed clusters remain auditable; replay inputs
  retain the resulting sweep state in decisions.
- Added hermetic coverage for identity/window boundaries, exchange
  participation, directional and premium boundaries, transition uniqueness,
  session gate separation, freshness, corrections, uncorrelated corrections,
  final audit freezing, and freshness expiry for closed clusters. Local
  verification: 69 tests passed, Ruff
  format/lint, and strict mypy passed.
- The bounded live smoke must not claim sweep qualification unless the feed
  naturally produces one. Dynamic ThetaData discovery remains a separate
  external HTTP 500 blocker and was not retried. Trading remains disabled;
  orders remain unconstructed and unsubmitted. PR #3 remains draft and
  unmerged. Recommendation: `STILL BLOCKED` pending the external discovery
  provider blocker and independent review of this closure.

## Independent sweep review corrections — 20260805

- Corrected F to use only the typed
  `most_recent_qualifying_sweep_timestamp`, with an exclusive 30-minute
  boundary; first-signal and equity timestamps no longer determine F.
- Corrected audit ordering so qualification state is finalized before every
  projection or transition record. Corrections replace the target constituent
  while retaining the original trade ID, corrective record ID, and both full
  records; cancellations remove the target and retain both records separately.
- Persisted deterministic sweep timer events carrying expiry timestamps.
  Replay schedules ticks from those events, and timer-driven expiry decisions
  now match field by field. Sweep audits are written through the repository in
  synchronous and asynchronous modes.
- Added append-only `sweep_transitions` history while retaining the latest
  `sweep_clusters` projection. Added the versioned numeric ThetaData exchange
  mapping `thetadata-opra-exchanges-v1`; unknown identifiers are retained in
  audit data but cannot count toward participation.
- Added explicit negative and positive boundary coverage for exchange
  validity, 65%/64.99% ask share, exact expiry, session reset, corrections,
  out-of-order events, ordered transition history, nonqualifying audits,
  synchronous/asynchronous writes, timer replay, and pipeline F re-evaluation.
  Local verification: 69 tests passed, Ruff format/lint, and strict mypy.

## Sweep live verification — 20260805

- Run `9b88bcab-d37b-4e2e-b047-35fd1e402c02` ran during regular hours with
  historical bootstrap success for AAPL/SPY/XLK, Alpaca authentication
  success, ThetaData authentication and exact-contract acknowledgement
  success, and the supplied AAPL 2026-08-07 $310 call only. No
  `STREAM_BULK` was used and no qualifying sweep naturally occurred.
- Sanitized counts: Alpaca 9,222 events; ThetaData 44 option events; trades
  received/processed 98/98; stale/late/duplicate/out-of-order 0/0/0/0;
  evaluations triggered/skipped 9/82; candidates evaluated/passing S/passing
  S+F+R/executable `1/0/0/0`; orders constructed/submitted `0/0`.
- Quote lifecycle counters were received 9,168, current-state overwrites
  9,166, consumed 90, pending at shutdown 1; overwrite operations are not a
  lifecycle bucket and `consumed + pending <= received` holds. Unknown
  conditions: 15. Provider and processing distributions were emitted
  separately; internal queue-wait p50/p95 was below 0.1/0.2 ms and processing
  p95 was below 0.1 ms for Alpaca quotes and 1.2 ms for Alpaca trades.
- Run-scoped PostgreSQL contained 98 events, 102 consumed-quote records, 9
  decisions, and 5 sweep audit rows. Replay produced 9 decisions with
  field-by-field equality. Dynamic discovery remains an external HTTP 500
  blocker and was not retried.

## Sweep review correction live verification — 20260805

- Corrected-head run `fe7fe601-bbf2-4f4b-b0d7-619e3e62c7ca` ran during regular
  hours at implementation head `fe2cd46c9979ec352397d1dab96b5b459035a1b9`.
  Historical bootstrap, provider authentication, exact-contract
  acknowledgement, and MDDS connectivity succeeded; `STREAM_BULK` was not
  used. No natural qualifying sweep occurred.
- Alpaca/ThetaData events were `7,028/96`; trades received/processed `152/152`;
  stale/late/duplicate/out-of-order `0/0/0/0`; evaluations triggered/skipped
  `26/109`; candidate counters `1/0/0/0`; unknown conditions `22`; orders
  constructed/submitted `0/0`.
- PostgreSQL contained 152 events, 170 consumed-quote records, 26 decisions,
  12 sweep projections, and 11 immutable transitions. No timer event was
  generated because no qualifying sweep occurred. Replay produced 26
  decisions with field-by-field equality. CI and all local checks passed.

## Final two sweep-review corrections — 20260805

- Removed fixture-only `A`/`B`/`C` aliases from the production ThetaData
  exchange mapping. All ordinary sweep fixtures use authoritative numeric
  identifiers; aliases, missing values, and unknown numeric values remain
  audit-visible but cannot satisfy exchange participation.
- Added exact freshness-boundary coverage at each qualifying sweep's most
  recent constituent timestamp plus 30 minutes, retaining just-before and
  just-after coverage. The pipeline timer persists and replay reproduces the
  expiry decision field by field.
- Focused implementation commit `5989744bd8caa0c3e5150927cd93d2f36cd928eb`
  is pushed. Local verification passed: Ruff format/lint, strict mypy on
  `src`, and 70 hermetic tests. GitHub Actions push run `31040159885` and PR
  run `31040159733` passed on that exact head. PR #3 remains draft and
  unmerged; trading remains disabled.

## Phase 3 closure and Phase 4 initialization — 20260805

- PR #3 was marked ready and merge-committed using the established merge
  method. Resulting `main` commit: `bbe969bf1d0df3b1dcdae6a2b4bd4ebb48899aeb`.
  The Phase 3 branch history and journals were retained.
- Phase 4 branch `feat/phase-4-live-universe` is based on synchronized `main`.
  Draft PR #4 is the live multi-symbol universe validation workstream.
- Added bounded pilot eligibility, deterministic v3 expiration/strike discovery,
  liquidity/moneyness selection, deduplication, Standard individual-subscription
  planning, 15,000-contract enforcement, reconciliation, and replayable universe
  audit persistence. No live-market validation was run.
- One sanitized off-hours diagnostic at `2026-08-05T20:28:59Z` found MDDS
  `CONNECTED` (HTTP 200), while contract-list, expiration, and strike helpers
  returned HTTP 500 HTML responses. Theta Terminal startup version was
  `20260803:9017549`. Dynamic live discovery remains blocked pending provider
  support; the expiration-plus-strike workflow is fixture-tested.
- Local Phase 4 preparation checks pass: Ruff format/lint, strict mypy on `src`,
  and 77 hermetic tests. Trading remains disabled; no orders were constructed
  or submitted. Preparation commit `df034e485edf48c4b082a22e40a62ce67cfbebb4`
  is published in draft PR #4. GitHub Actions run `31044392765` passed on that
  head. Next-session command is documented in `docs/phase4/README.md`.

## Phase 4 Alpaca catalog and acknowledgement update — 20260806

- PR #4 remains draft and unmerged on `feat/phase-4-live-universe`.
- Added paginated Alpaca active-call contract discovery, immutable original
  field capture, exact Decimal OCC/canonical/ThetaData mapping, independent OCC
  decoding, round-trip validation, deterministic DTE/moneyness/liquidity
  selection, 15,000-contract overflow failure, universe manifests, and
  acknowledged-contract event gating.
- Theta acknowledgements now follow the support-confirmed shape exactly:
  `header.type=REQ_RESPONSE`, `header.req_id`, and `header.response`. STATUS
  frames are independent keepalives; no acknowledgement contract is expected.
- After a clean launcher restart with debug logging, active Terminal build was
  `20260804:bdd51ae` at `2026-08-06T13:23:37Z`; MDDS was CONNECTED. One bounded
  diagnostic returned contract-list HTTP 472 JSON and expiration/strike HTTP
  500 HTML. Debug evidence identified a zstd JNI load failure under `/tmp`
  restrictions. REST discovery remains cross-validation-only and was not retried.
- Local verification at this pass: 98 hermetic tests, Ruff, and strict mypy.

### SESSION-0014 closure evidence — 20260806

- Final head: `2cc3759b21ea977fa07d1d4cb57845f839edc22a3` on
  `feat/phase-4-live-universe`; PR #4 remains draft and unmerged.
- The implementation uses Alpaca’s documented options-contract filters with
  explicit 7–45 DTE bounds, accepts compact and padded OCC representations, and
  gates ThetaData data on correlated `REQ_RESPONSE` acknowledgements. Local
  verification passed with 99 hermetic tests, Ruff, and strict mypy.
- Regular-session pilot run `87a96eb6-b0ff-4bd9-bfca-0748eced65df` received and
  mapped 11,956 contracts, rejected none, persisted the universe manifest,
  selected no contracts, and opened no ThetaData subscriptions. All 20 symbols
  failed closed because dated open-interest or average-options-volume evidence
  was unavailable. Orders remained 0/0 and trading remained disabled.
- This is not a live Phase 4 observation: no contract plan existed, so no
  ThetaData acknowledgements, events, qualifying sweeps, or replay metrics were
  produced. Smallest next action: add a typed Alpaca liquidity-evidence source
  or obtain owner approval for an equivalent evidence field. ThetaData REST
  discovery remains unavailable and was not retried.

### SESSION-0015 precision amendments — 20260806

- Implemented policy `phase4-observation-liquidity-relaxation-v1` only in the
  Phase 4 Alpaca selector. Each symbol records
  `symbol_liquidity_evidence_source=OWNER_APPROVED_PHASE4_PILOT` and
  `symbol_liquidity_verified=false`; selected contracts inherit this
  provenance. The policy cannot be selected under another version.
- Phase 4 accepts positive undated OI observationally, preserving
  `oi_date_source=ALPACA_UNDATED`, `open_interest_verified_as_of=false`, and
  `evidence_quality=PHASE4_OBSERVATIONAL` through decisions. Missing or zero OI
  remains a fail-closed reason.
- Added separate deterministic TRADE/QUOTE capacity budgets, per-symbol caps,
  round-robin allocation, and persisted capacity exclusions with rank and
  `SUBSCRIPTION_CAPACITY_EXCLUDED` reason. Counts are reported separately.
- Local verification: 104 hermetic tests, Ruff format/lint, and strict mypy.
  No live run was performed for this amendment; PR #4 remains draft and
  unmerged, trading remains disabled, and no orders are constructed/submitted.

### SESSION-0015 regular-session smoke — 20260806

- Run `cc56f1d3-6dca-411b-ad46-fd9f9b84888e` received/mapped 11,956 contracts,
  with zero mapping failures. All symbols failed closed for missing or zero OI
  and unavailable quote liquidity; the owner-approved average-volume
  relaxation did not override those mandatory requirements.
- Requested, selected, capacity-excluded, submitted, acknowledged, and
  rejected subscription counts were all zero because no contract passed the
  evidence filters. No ThetaData stream, sweep, or decision evidence was
  produced. Orders remained 0/0 and trading remained disabled.
- Correction: the preceding pre-smoke note saying no live run was performed is
  superseded by this regular-session policy smoke; no stream opened because
  every symbol failed the mandatory evidence filters.

### SESSION-0016 liquidity enrichment — 20260806

- Added coarse shortlist, Alpaca OPRA/indicative snapshot evidence, ThetaData
  dated OI evidence, exact identity joining, quote freshness/spread/size checks,
  previous-session effective dates, persisted exclusions/enrichment records,
  and final selection gating. Local verification passed with 110 hermetic
  tests, Ruff, and strict mypy.
- Regular-session run `97071de6-f0bb-456c-b3ac-2bbf45036ff1` produced a coarse
  shortlist of 500 contracts. The single bounded AAPL OI diagnostic returned
  HTTP 500 from `/v3/option/snapshot/open_interest`; MDDS was CONNECTED and the
  active Terminal build was `20260804:bdd51ae`. No further OI requests or
  streams were made; orders remained 0/0.
- Status remains blocked pending ThetaData restoration of the documented OI
  snapshot endpoint. Discovery endpoints were not retried.

### SESSION-0017 closure — owner-authorized observation without OI — 20260806

- Commits `97234b2`, `2b34851`, `b4c36cd`, and `20d798a` implement the
  owner-authorized separation. The Phase 4-only policy is
  `phase4-sweep-observation-without-oi-v1`; quote-liquid canonical calls may
  be observed without OI, while missing OI remains null for the ratio and
  blocks complete signal eligibility and executable candidates.
- Corrected Alpaca snapshot parsing for the documented `snapshots` envelope.
  Corrected PostgreSQL flushing and manifest-shape handling so full universe
  manifests are queryable under the sanitized `__MANIFEST__` audit key.
- Thirty-minute regular-session run
  `64b167fe-6bf5-4f37-a686-267dbca1cdc7` mapped 11,956 contracts, selected 10
  quote-liquid observation contracts across BAC, NFLX, NVDA, and TSLA, and
  submitted/acknowledged 10 TRADE plus 10 QUOTE subscriptions. It received
  165,144 ThetaData events and 874,245 Alpaca events; trades received and
  processed were both 10,025. It recorded 1,029,364 quotes received, 7,280
  consumed, and 16 pending at shutdown. Stale, late, duplicate, and
  out-of-order counts were all zero; unknown-condition count was 154.
- No qualifying sweep and no complete S/F/R/E signal were observed. OI was
  unavailable; no OI-dependent signal is claimed. Orders constructed/submitted
  remained 0/0, trading remained disabled, and the prior sanitized OI HTTP 500
  was not retried. The short corrected persistence verification stored one
  complete universe manifest row; its event and decision counts were zero
  because it ended during startup/drain.
- Local verification: Ruff format/lint, strict mypy, and 114 hermetic tests.
  Exact-head GitHub Actions passed on `20d798a` in runs
  `31115127177` (push) and `31115131804` (pull request). PR #4 remains draft
  and unmerged. ThetaData OI snapshot availability and REST discovery remain
  external provider blockers, but do not block owner-authorized observation.

### SESSION-0018 extended observation — 20260806

- The required-head observation launched at 13:18:23 ET completed normally at
  approximately 15:49:18 ET. Run ID `ee806be1-c9d3-4917-b0a6-8e38054cf04d`
  reported `live_observation_complete`; no restart was performed.
- The deterministic policy selected 14 contracts across BAC, NFLX, and NVDA.
  TRADE and QUOTE requests were each submitted/acknowledged `14/14`, with no
  request rejection. Provider authentication succeeded and MDDS remained
  CONNECTED.
- Alpaca/ThetaData event counts were `1,896,125/437,969`; accepted trades
  received/processed were `31,596/31,596`. Quotes received, overwritten,
  consumed, and pending were `2,302,498/2,302,479/22,960/18`. Stale, late,
  duplicate, and out-of-order counts were `0/0/0/0`; unknown conditions were
  `302`. The acknowledged-contract registry separately rejected `459,009`
  unacknowledged data messages.
- PostgreSQL contained `31,596` canonical events, `31,568` quote consumptions,
  `221` sweep clusters, `208` transitions, and `0` decisions. Replay returned
  `31,596` events and `0` decisions; decision field equality is vacuous because
  no synchronized decision was created. The required historical head did not
  persist a universe manifest.
- No qualifying sweep occurred. The closest clusters passed five of six
  cluster thresholds but failed cluster premium: NVDA 20260814/250000C at
  `$450`, and NFLX 20260814/77000C at `$1,804`; other near clusters also failed
  exchange participation. S/F/R/E evaluations and candidate counters were all
  zero because synchronized inputs were zero; missing OI remained unavailable
  and no complete signal is claimed. Orders remained `0/0` and trading stayed
  disabled.
- Provider-age p50/p95 (ms): Alpaca quotes `21.08/56.37`, Alpaca trades
  `21.74/69.17`, Theta quotes `30.45/79.91`, Theta trades `33.46/121.16`.
  Internal queue-wait p50/p95 (ms): `0.053/0.115`, `0.057/0.114`,
  `0.138/0.207`, and `0.127/0.189`, respectively. Database-write latency
  p50/p95/max was `21.86/32.30/554.39 ms`; queue depth p50/p95/max was
  `1/10/83`; no soft or hard backpressure failures occurred.

### SESSION-0017 owner-directed observation eligibility — 20260806

- Phase 4 now separates observation-subscription eligibility from complete
  S/F/R/E eligibility. Quote-liquid, round-trip-validated active calls may be
  subscribed for sweep observation without OI under policy
  `phase4-sweep-observation-without-oi-v1`; the policy is Phase 4-only and does
  not authorize orders or complete signals.
- ThetaData dated OI remains preferred optional enrichment. The known sanitized
  HTTP 500 is recorded once and the live runner supports an explicit
  `--skip-oi-diagnostic` mode so today’s observation does not retry that endpoint.
  Missing OI remains `null` for the OI ratio, blocks the OI-dependent gate with
  `OPEN_INTEREST_UNAVAILABLE`, and keeps executable candidates at zero.
- Added regression coverage for missing/undated Alpaca OI, optional observation
  selection, and null OI-dependent signal ratios. Local checks currently pass
  with 113 hermetic tests, Ruff format/lint, and strict mypy; live observation
  and exact-head CI remain pending.

### SESSION-0019 Phase 4 coverage and synchronization correction — 20260806

- Corrected the historical live finding from run
  `ee806be1-c9d3-4917-b0a6-8e38054cf04d`: it completed normally, processed
  `31,596/31,596` accepted trades, and formed `221` clusters, but produced zero
  synchronized inputs and zero S/F/R/E evaluations because the smoke path
  requested AAPL while only BAC, NFLX, and NVDA were selected. OI was
  unavailable/incomplete but was not the primary synchronization cause. No live
  strategy decision or complete signal was demonstrated; replay of zero
  decisions was vacuous.
- The pipeline now synchronizes each selected option root with its own equity
  state, SPY benchmark, configured sector ETF, option flow, and sweep state.
  Missing state is reported per symbol, and decision provenance preserves the
  evaluated symbol. AAPL is not required unless selected.
- Phase 4 pre-enrichment is now capped at 100 contracts per symbol and 2,000
  total candidates. Observation spread filtering is explicitly versioned as
  `phase4-observation-spread-v1` with absolute `$0.05` and proportional `20%`
  limits; production signal thresholds remain unchanged. Diagnostics preserve
  formula, threshold, and price/spread buckets.
- ThetaData rejected contract events now produce bounded, run-scoped aggregate
  diagnostics with membership flags and an honest overflow count; rejected
  events never enter strategy state. Universe manifests now include the engine
  commit, policy/provenance, synchronization state, request registry and
  acknowledgement evidence, enrichment diagnostics, and rejected-event
  aggregates.
- Deterministic verification passed locally: 118 hermetic tests, Ruff format,
  Ruff lint, and strict mypy. Exact-head CI run
  [31127740251](https://github.com/olubadan/institutional-signal-engine/actions/runs/31127740251)
  passed all jobs on the pushed implementation head. No new live observation or
  provider REST retry was performed; PR #4 remains draft and unmerged, trading
  remains disabled, and orders remain 0/0.

### SESSION-0020 startup-boundary correction — 20260807

- The prior 600-second Phase 4 smoke started but remained alive for about
  10m41s with a zero-byte protected log, no run ID, and no report. Because the
  process emitted no progress or stack evidence before it was stopped, the
  exact blocked provider operation cannot be retrospectively identified from
  that run; no live observation was restarted.
- Added flushed, sanitized startup stage records covering configuration,
  database, provider authentication, discovery, enrichment, selection,
  subscription planning, websocket connection, acknowledgements, observation,
  persistence drain, and final report emission. Sensitive-looking fields are
  redacted before output.
- Added a configurable global startup budget (`PHASE4_STARTUP_TIMEOUT_SECONDS`,
  CLI `--startup-timeout-seconds`) around Alpaca historical/pricing and paged
  contract calls, quote enrichment, database initialization, MDDS status, and
  subscription acknowledgement. The CLI also has an outer total-command
  timeout, so startup cannot run indefinitely; timeout records contain only
  stage, elapsed time, counts, and sanitized error category.
- The signal smoke now waits for correlated ThetaData acknowledgements before
  emitting `observation_started` or starting equity collection, so the
  requested observation timer begins after startup succeeds. Accepted writes
  are drained on acknowledgement timeout before the bounded failure exits.
- No strategy, selection, spread, sweep, gate, discovery endpoint, OI endpoint,
  threshold, or live-observation result was changed. Trading remains disabled
  and orders remain 0/0.
- Verification passed locally: Ruff format check, Ruff lint, strict mypy,
  `uv lock --check`, shell syntax, structural/secret scan, and 122 hermetic
  tests. ShellCheck was unavailable locally and remains covered by CI.
- Implementation commit `190574d73d101d3b422178799ceb0e96d68a545f` is pushed;
  exact-head GitHub Actions run
  [31190825701](https://github.com/olubadan/institutional-signal-engine/actions/runs/31190825701)
  passed every verification job. PR #4 remains draft and unmerged.

### SESSION-0021 Phase 4 closure — 20260807

- Validated implementation head is
  `88763d451bf7b07033eaec999d106879ecfaa285`. Extended run
  `9c95f8f0-ae8f-4194-9152-0c33552bd170` completed naturally from the
  protected operational log; no further observation is required.
- The live pilot selected 92 call contracts across 12 symbols. TRADE and QUOTE
  subscriptions were each requested and acknowledged `92/92`, with zero
  request rejections. Accepted trades received/processed were
  `24,255/24,255`; synchronized inputs were `30,009`; evaluations and
  PostgreSQL decisions were `4,125`.
- Run-scoped PostgreSQL replay loaded `24,255` events and `193,755` consumed
  quote records, replayed `4,125` decisions, and achieved field-by-field
  equality. Cross-symbol mismatches were zero. The final universe manifest
  projection records the exact engine commit, 92-contract plan, 184
  acknowledgements, 81 bounded rejected-message aggregates, and 12
  synchronization symbols.
- The run formed `1,799` sweep clusters and produced zero qualifying sweeps.
  This is an honest market observation: the pipeline synchronized, evaluated,
  persisted, and replayed successfully, while no cluster satisfied all
  authoritative thresholds, principally cluster premium and three-exchange
  participation. Missing OI remained separate from observation eligibility
  and blocked only OI-dependent complete signal fields.
- Persistence soft-limit crossings and hard failures were zero. Trading stayed
  disabled and orders constructed/submitted remained `0/0`.
- ThetaData REST discovery/OI endpoints remain unavailable for future
  cross-validation, but are not a Phase 4 closure blocker and were not retried.
  Phase 5 must not inherit any observational relaxation automatically.
- PR #4 is open, ready for owner review, mergeable, and unmerged. The next
  action is owner review and merge authorization; Phase 5 begins only after
  the owner explicitly authorizes and completes the merge.

### SESSION-0023 Phase 4B system-integration closure — 20260809

- The outer Phase 4B runner now owns one run ID, carried through discovery,
  coverage, ordered allocation, requests, acknowledgements, and persisted
  finalization. The planner is the sole allocator; the subscription formatter
  only validates and serializes its ordered result.
- Every discovered contract receives immutable transition evidence, including
  later-expiration, duplicate-canonical, mapping, enrichment, U_X, planner,
  capacity, request, and acknowledgement outcomes. Canonical-null records
  carry `CANONICAL_IDENTITY_UNAVAILABLE`.
- Finalization replay validates the persisted selection, allocation, request
  subsets, acknowledgement lineage, and transition order. The read-only
  `python -m institutional_signal_engine.ignition` command provides sanitized,
  fail-closed deployment checks and never starts an observation.
- Closure verification is hermetic: 162 tests pass, Ruff format/lint and
  strict mypy pass. No provider ran in this session. Remaining follow-up is
  expanded failure injection and live validation; market-wide cataloging and
  a universal Q-delta bound remain out of scope. Trading is disabled and
  orders remain `0/0`.

### SESSION-0025 Phase 4B deterministic certification harness — 20260810

- Work Package 1 adds the versioned `PHASE4B_CERT_V1` contract, JSON Schema,
  deterministic accelerated-RTH scenario, offline certificate runner, focused
  tests, and concise certification boundary document.
- Command: `uv run python -m institutional_signal_engine.phase4b_certify`.
  The expected current result is nonzero `FAIL`; the certificate identifies
  planner epoch immutability, exact planner-epoch consumption, and duplicate
  discovery/enrichment as failed production integration invariants.
- The harness exercises production impact scoring, coverage allocation, async
  in-memory persistence, and replay without running live providers. Trading is
  disabled and orders remain `0/0`.
- Verification: Ruff, strict mypy, and 168 hermetic tests passed; focused
  schema/structural checks, secret scan, and diff check passed. `jsonschema`
  was not installed; dependency-free certificate schema validation passed.

### SESSION-0027 final causal-journal correction — 20260811

- Draft PR #6 now uses one `OrchestrationShell` for production and deterministic
  execution. A driver selects market events, scheduled clock boundaries,
  disconnects, and intake stop as independent inputs.
- The runtime certificate accepts one sealed `VerifiedJournal` and reads no Git,
  worktree, shell, repository, environment, scenario helper, or expected-result
  object. Build-envelope evidence is separate.
- Journal verification recalculates payload and complete-record digests, the
  ordered chain, parent constraints, run and sequence consistency, and the seal;
  semantic verification enforces discovery/enrichment, epoch, paired command and
  acknowledgement, activation, event, restoration, clock, and terminal lifecycle
  causality.
- Canonical journal persistence is reconstructed through the repository boundary;
  the replayed journal is reverified and projects an exactly equal certificate.
  The deterministic trace proves E1 `{A}`, E2 `{A,B}`, E3 `{B}`, clock-only
  reevaluations, recovery, removed-A rejection, active-B acceptance, stop, drain,
  finalization, persistence, and replay.
- The focused positive and corruption suite has 25 tests; the complete hermetic
  suite has 239 tests. Implementation commit
  `4cfdd92c51e627d37d117f176e60a558ca5071b5` and both implementation-head CI
  triggers passed. Trading is disabled and orders remain `0/0`; artifact digests
  and the journal-close state are recorded in SESSION-0027.

### SESSION-0028 final evidence-bundle correction — 20260811

- The circular post-finalization journal proof is removed. Runtime evidence now
  follows `J = seal(compose(E_det, Sigma))`, `P = persist(J)`, `R = replay(P)`,
  `Omega = (J, P, R)`, and `Certificate = pi_CDelta(Omega)`.
- J ends at `session.finalized` and contains no persistence or replay success
  claim. P and R are separate immutable, recalculably digested receipts. P uses
  a content-addressed repository identity; replay reconstructs from that object
  and compares exact bytes, counts, roots, and observed projection digests.
- Immutable `PHASE4B_ACCEPTANCE_CONTRACT_V3` owns expected values and rules.
  J/P/R own observations. The certificate visibly separates the two and contains
  no Git, worktree, CI, PR, shell, or environment evidence.
- Exact operation, cause-operation, and correlation identities plus a closed
  material vocabulary reject coherent fabrications, wrong same-kind parents,
  duplicates, contradictions, misplaced records, wrong epochs/restorations/
  boundaries, forged receipts, cross-run or cross-version evidence, and
  post-finalization records.
- Local verification passes: Ruff format/lint, strict mypy, dependency lock,
  structural and secret checks, 258 hermetic tests, and 44 focused production-
  path positive/adversarial tests. Three executions produced byte-identical
  artifacts. J/P/R/certificate SHA-256 values are respectively
  `02fa0820152556db2b6a3620224a77865238a393e83536a39399d886406522c1`,
  `c97d148f7bbae485be231df2d45714ee749ac80a7de58c3a56a2e96142369835`,
  `5c8faad0f1c396d3c4ad00965365a4ca156233e89b112e06f3a469b352f524b1`,
  and `01dbcdcaca2e367aeac5db63bd9b3b4a1818e357d428215b4af9ec1111e40784`.
- No provider ran. Trading remains disabled and orders remain `0/0`. PR #5 is
  unchanged; PR #6 remains draft and unmerged. The exact journal-closing commit
  and exact-head CI are recorded in GitHub PR evidence and the terminal handoff.

### SESSION-0029 repository-authority correction — 20260811

- Runtime certification now follows `J = seal(compose(E_det, Sigma))`,
  `P = persist(J)`, `R = replay(P)`,
  `Omega_verified = verify_repository_backed(CDelta, J, P, R, repository)`, and
  `Certificate = pi_CDelta(Omega_verified)`.
- The production verifier receives the persistence repository, loads P's exact
  content-addressed object, deserializes and fully verifies a distinct journal,
  enforces exact canonical-byte equality with J, derives replay facts from that
  reconstruction, and compares caller-supplied R rather than trusting it.
- The verified evidence package is an opaque capability issued only by that
  repository-backed path. Projection rejects direct construction and unverified
  objects. Missing objects fail as `PERSISTED_JOURNAL_MISSING`; stored-byte
  divergence and invented replay facts have stable specific codes.
- Local verification passes: Ruff format/lint, strict mypy, dependency lock,
  structural and secret checks, Bash syntax, 267 hermetic tests, and 53 focused
  production-path positive/adversarial tests. Local ShellCheck was unavailable;
  exact-head GitHub CI installs and runs it. Two executions produced byte-identical
  artifacts. J/P/R/certificate SHA-256 values remain respectively
  `02fa0820152556db2b6a3620224a77865238a393e83536a39399d886406522c1`,
  `c97d148f7bbae485be231df2d45714ee749ac80a7de58c3a56a2e96142369835`,
  `5c8faad0f1c396d3c4ad00965365a4ca156233e89b112e06f3a469b352f524b1`,
  and `01dbcdcaca2e367aeac5db63bd9b3b4a1818e357d428215b4af9ec1111e40784`.
- No provider ran. Trading remains disabled and orders remain `0/0`. PR #5 is
  unchanged; PR #6 remains draft and unmerged. Exact-head publication and CI are
  confirmed externally after the single correction commit is pushed.

### SESSION-0030 fused certification boundary correction — 20260811

- Authoritative issuance now has exactly one production construction function:
  `Certificate = certify_repository_backed(CDelta, J, P, R, repository)`.
  `VerifiedEvidencePackage`, `_issue`, the repository-verification authority,
  authority checks, the standalone projection, and the repository-free artifact
  constructor are removed.
- Every certification invocation independently validates sealed J, validates P
  only as a claim, loads P's exact repository object, compares canonical bytes,
  reconstructs and fully verifies a distinct journal, derives observations and
  all replay facts from verified local objects, compares caller R, and constructs
  the certificate immediately. Certificate observations use the repository
  reconstruction and locally derived P/R facts, never caller fields directly.
- `validate_certificate_schema` is explicitly shape-only and cannot authorize
  issuance. Public-API and AST inspection prove the removed names are absent and
  only `certify_repository_backed` contains an `overall: PASS` construction.
- Adversarial production-boundary coverage includes former-capability lookalikes,
  low-level object mutation, copied evidence, forged authority identity, empty,
  wrong, altered, cross-run and cross-contract repository evidence, invented R,
  schema-valid manual fields, residual projection API absence, and mandatory
  repository loading with stable failure codes.
- Local verification passes: Ruff format/lint, strict mypy, dependency lock,
  structural and secret checks, Bash syntax, 271 hermetic tests, and 57 focused
  certification tests. Local ShellCheck was unavailable; exact-head GitHub CI
  installs and runs it. Two executions produced byte-identical artifacts.
  J/P/R/certificate SHA-256 values remain respectively
  `02fa0820152556db2b6a3620224a77865238a393e83536a39399d886406522c1`,
  `c97d148f7bbae485be231df2d45714ee749ac80a7de58c3a56a2e96142369835`,
  `5c8faad0f1c396d3c4ad00965365a4ca156233e89b112e06f3a469b352f524b1`,
  and `01dbcdcaca2e367aeac5db63bd9b3b4a1818e357d428215b4af9ec1111e40784`.
- No provider ran. Trading remains disabled and orders remain `0/0`. PR #5 is
  unchanged; PR #6 remains draft and unmerged. Exact-head publication and CI are
  confirmed externally after the single correction commit is pushed.

### SESSION-0031 complete certification-authority closure — 20260811

- The sole authoritative interface is now
  `generate_phase4b_certification(output_paths) -> CertificationArtifacts`.
  Its only input contains four presentation destinations. It accepts no
  contract, J/P/R, repository, factory, scenario, event, expectation, callback,
  cached value, capability, certificate, or result.
- One call reads the exact fixed package contract bytes and verifies pinned
  SHA-256
  `a68e3d894bd29e1ace5dd84dfefab87fe8609c2adae93e9c640c105426e23c9a`,
  fully validates the contract, verifies the checked-in scenario digest
  `05ac756c035d8aca12fe8ff5d9016024ef814b512166c8ceecaed40a318b593f`,
  constructs the scenario and owned repository, runs the real orchestration,
  seals/verifies J, saves/reloads P, reconstructs and independently verifies R,
  immediately projects the certificate, then stages and publishes J/P/R before
  publishing the certificate last.
- The certificate visibly binds the contract and scenario identities, run,
  journal root/bytes/count/seal, persistence identity/receipt, replay receipt,
  reconstructed root/bytes/count/projections/equality, observed projection,
  schema version, and canonical certificate commitment. Build-envelope facts
  remain outside runtime evidence.
- Former caller-driven certify/composition/replay/load/write paths are absent.
  Journal/contract/receipt constructors, deserializers, hashing, and low-level
  verifiers are explicitly non-authoritative and have no certificate consumer.
  `validate_certificate_schema` remains shape-only; no production artifact
  reader relabels a manual document authoritative.
- The final focused suite has 65 tests and the complete hermetic suite has 279.
  Two fresh post-implementation construction-graph enumerations found no new
  bypass. Two independent executions produced byte-identical J/P/R/certificate
  hashes respectively
  `02fa0820152556db2b6a3620224a77865238a393e83536a39399d886406522c1`,
  `c97d148f7bbae485be231df2d45714ee749ac80a7de58c3a56a2e96142369835`,
  `5c8faad0f1c396d3c4ad00965365a4ca156233e89b112e06f3a469b352f524b1`,
  and `e5e1286264614184c7a80cbe23426eff4b0ae7616fb57a2588230679d36ce5e8`.
- No provider ran. Trading remains disabled; orders remain `0/0`; PR #5 remains
  unchanged; PR #6 remains draft and unmerged; Phase 5 has not started.

### SESSION-0032 live observational path closure — 20260811

- Added a separate live observational evidence subsystem in
  `live_session.py`. It requires a fresh output directory, configured durable
  journal storage, a durable application repository, a protected authority key,
  exact Git head, and disabled trading before provider connection.
- One unified session port now owns discovery-adjacent subscription commands,
  correlated paired acknowledgements, inbound events, clock boundaries,
  disconnect/reconnect restoration, 16:00 ET intake stop, drain, finalization,
  terminal journal seal, durable persistence, replay, and manifest-last staged
  publication. Incomplete or partially written runs remain distinguishable and
  cannot publish a certificate.
- The live manifest binds commit, application/config/schema/rules identities,
  configuration snapshot, session boundaries, provider identities, journal
  count/root/seal, connection/recovery history, event and epoch evidence,
  persistence/replay receipts, host-clock observations, impact fallback, and
  disabled trading/order state. An HMAC authority binding rejects coherent
  rehashing without the trusted deployment key.
- Added offline `inspect`, `replay`, and `certify` commands. The live certificate
  states that one persisted observational session was validated offline and
  explicitly records that the deterministic hermetic scenario did not run.
  The accepted `phase4b_certify` subsystem and its authority boundary remain
  unchanged.
- Added a deterministic virtual-clock harness using the same live composition;
  it covers `{A} -> {A,B} -> {B}`, paired acknowledgements, events, removal,
  disconnect/reconnect restoration, 16:00 stop, durable persistence, replay,
  and live certification. Two executions produced byte-identical deterministic
  bundle, replay, and certificate artifacts.
- Verification currently passes: Ruff format/lint, strict mypy, lockfile check,
  Bash syntax, structural/secret/diff checks, and 287 hermetic tests including
  9 focused live-session adversarial tests. No provider ran; trading remains
  disabled; orders remain `0/0`; PR #5 is unchanged; PR #6 remains draft and
  unmerged; Phase 5 has not started.
- Implementation commit `1205afcafe612958d37789965f01f3bf704da7b9` is pushed to
  PR #6's branch. Exact-head CI jobs
  `31480520321` and `31480517224` passed. PR #6 remains open, draft, mergeable,
  and unmerged; PR #5 remains unchanged, open, and draft at
  `4a3afe8dd70fecc1b629ff7ef5614a652a572529`. The live path build is complete
  and awaits independent validation; this is not a provider or host readiness
  claim.
