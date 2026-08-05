# Current State

- **Project mission:** Build a production-oriented, deterministic, real-time
  institutional signal engine using Alpaca equity data and paper execution,
  a replaceable external options-data provider, synchronized normalized events,
  formal S/F/R/E gates, deterministic ranking, traceable persistence,
  observability, replay, and reproducible Vast.ai operation.
- **Current phase:** Phase 3 — signal-only vertical slice in progress on
  `feat/signal-only-vertical-slice`
- **Completed phases:** Phase 0 — Ground the Current State; Phase 1 — canonical
  documentation baseline; Phase 2 — reproducible Vast environment verified with
  documented Ubuntu 22.04/finite-duration exception and merged in PR #2
- **Active branch:** `feat/signal-only-vertical-slice`
- **Latest authoritative main commit:** `a3b34c5166040797123e943c23f0428344198ac3`.
  Phase 2 implementation is verified through
  `1b2920ce7a6a305c9b9cfa4f395b4f06862d1a68`; the active branch head after
  session closure is the journal-close commit containing this snapshot.
- **Pull-request state:** PRs #1 and #2 are merged into `main`; Phase 3 PR
  [#3](https://github.com/olubadan/institutional-signal-engine/pull/3) is open,
  draft, cleanly mergeable, and unmerged at the SESSION-0010 correction head.
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
- **Next action:** Obtain independent review of PR #3. Keep it draft and
  unmerged; dynamic ThetaData discovery remains the only provider blocker.

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
  verification: 68 tests passed, Ruff
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
  Local verification: 68 tests passed, Ruff format/lint, and strict mypy.

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
