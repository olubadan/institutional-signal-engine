# System Requirements Specification

## Functional requirements

| ID | Requirement |
| --- | --- |
| FR-DATA-001 | Receive resilient real-time equity market data from Alpaca. |
| FR-DATA-002 | Receive resilient real-time options data through a replaceable provider adapter. |
| FR-DATA-003 | Normalize symbols, timestamps, prices, equity volume, option volume, open interest, call premium, spread, market state, sector state, and provenance. |
| FR-DATA-004 | Synchronize streams under explicit lateness, ordering, heartbeat, reconnection, backoff, staleness, and rate-limit policies. |
| FR-SIG-001 | Compute every indicator and decision in the authoritative 14-part formal model without semantic modification. |
| FR-SIG-002 | Evaluate signal validity `S`, freshness `F`, room/capacity `R`, executability `E`, and candidate set `C(t)`. |
| FR-SIG-003 | Rank candidates with the specified lexicographic ordering and emit a deterministic no-candidate decision. |
| FR-EXE-001 | Submit orders only to Alpaca paper trading and remain disabled by default. |
| FR-EXE-002 | Apply the specified entry protection, take-profit, position sizing, fixed-horizon, and freshness-loss behavior. |
| FR-EXE-003 | Provide a kill switch and enforce concurrent-position capacity. |
| FR-AUD-001 | Record normalized events, signal state, gate outcomes and reasons, candidates, decisions, order intent, acknowledgements, fills, rejections, exits, costs, and returns. |
| FR-AUD-002 | Trace each decision to exact normalized inputs, configuration version, threshold version, and engine version. |
| FR-OBS-001 | Expose health, readiness, feed health, signal counts, gate rejections, latency, staleness, orders, fills, exposure, execution errors, and daily performance. |
| FR-REP-001 | Record a versioned canonical event format and replay it deterministically through the same decision path. |

## Non-functional requirements

| ID | Requirement |
| --- | --- |
| NFR-DET-001 | Identical ordered events, configuration, and engine version produce identical ordered decisions. |
| NFR-TIME-001 | Internal timestamps are timezone-aware UTC and preserve source, receipt, normalization, and decision times. |
| NFR-REL-001 | External connections use bounded timeouts, classified retry behavior, backoff, heartbeat, and structured errors. |
| NFR-REL-002 | Missing, malformed, duplicate, stale, late, and out-of-order events have explicit deterministic treatment. |
| NFR-SEC-001 | Credentials, tokens, authorization headers, private keys, certificates, and populated secret files never enter Git or durable output. |
| NFR-SEC-002 | Runtime secrets use an approved secret mechanism or a mode-`0600` protected `.env` on Vast; secret files remain ignored. |
| NFR-SAFE-001 | Execution fails closed, trading defaults disabled, and no live-trading route is enabled. |
| NFR-PORT-001 | Provider SDK and wire types remain inside adapters; domain policy is provider-, transport-, database-, cache-, and framework-independent. |
| NFR-AUD-001 | Events and decisions are append-only or otherwise auditable with stable identifiers and version lineage. |
| NFR-ENV-001 | The approved persistent environment uses Ubuntu 24.04, at least 4 vCPU/16 GB RAM/100 GB storage, with 8 vCPU/32 GB preferred, reliable networking, Docker, Compose, Python 3.12, `uv`, Git, GitHub CLI, PostgreSQL client, Redis client, and build tools. |

## Configuration requirements

- Thresholds, symbol universe policy, allocation, horizon, capacity, and trading
  enablement are explicit, validated, and versioned.
- Configuration snapshots referenced by decisions exclude secret values.
- Provider-specific URLs and credentials are supplied only when their integration
  phase begins and are confirmed by presence/authentication status only.

## Secret-handling procedure

- Do not request credentials until required by an approved integration step.
- Tell the owner the exact variable names and secure entry location.
- Disable shell tracing around authentication and redact sensitive output as
  `[REDACTED]`.
- If exposure is suspected, stop, identify the location without reproducing the
  value, notify the owner, and recommend revocation and rotation. History rewriting
  or artifact deletion requires explicit approval.
