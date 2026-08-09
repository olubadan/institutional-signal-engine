# Phase 4B Monday preflight and observation runbook

This runbook is preparation only. Do not launch it during the weekend.

1. Earliest preflight: 09:00 America/New_York on the next regular-session
   date; do not start observation before 09:30 ET.
2. From the authenticated checkout, verify branch
   `feat/phase-4b-impact-shadow`, a clean tree, and `git rev-parse HEAD`.
   Confirm the same exact head is the draft PR head and exact-head CI is green.
3. On the existing VM, fast-forward only the clean checkout to that exact
   head using the approved bundle procedure if GitHub SSH access is absent.
4. Check PostgreSQL and Redis health. Check the active Theta Terminal startup
   build and loopback MDDS status `CONNECTED`.
5. Load the protected runtime through the Python configuration layer only;
   confirm provider authentication by sanitized status. Confirm
   `TRADING_ENABLED=false` and that no order route exists; orders must remain
   `0/0`.
6. Before market open, load completed split-adjusted Alpaca bars, calculate
   and freeze the five-minute baselines, and persist effective date, sample
   size, adjustment metadata, and provenance. Fail closed on fewer than 20
   completed sessions.
7. Generate and persist the conditional coverage sets `U_M`, `U_X`, `U_R`,
   independent TRADE/QUOTE capacity results, and every deterministic capacity
   exclusion. Do not retry ThetaData REST discovery or OI endpoints.
8. Verify the single shared provider/subscription/normalization/quote/
   persistence pipeline and the ordered individual-contract plan.
9. Subscribe individual ThetaData Standard TRADE and QUOTE streams; correlate
   only `REQ_RESPONSE.header.req_id` and validate incoming identities against
   the acknowledged registry. Never use `STREAM_BULK`.
10. Launch only after 09:30 ET in a protected tmux session:

```sh
RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
LOG="/var/log/phase4b-impact-shadow-${RUN_TS}.jsonl"
umask 077
tmux new-session -d -s phase4b-observation \
  "cd /opt/institutional-signal-engine && RUNTIME_ENV_FILE=/etc/institutional-signal-engine/runtime.env uv run python -m institutional_signal_engine.phase4_live_smoke --seconds 21600 --skip-oi-diagnostic >${LOG} 2>&1"
chmod 600 "${LOG}"
```

11. Health-check the tmux process and protected structured log without
   printing environment values. Record start/completion times and bounded
   drain status.
12. At completion, query only the run ID for manifest, accepted events,
   consumed quotes, impact clusters, sessions, decisions, queue metrics, and
   acknowledgement outcomes.
13. Replay the run from accepted events, consumed quotes, sweep audits, frozen
   baselines, timers, universe manifest, and model configuration. Compare
   shadow outputs and CONTROL_V1 outputs field by field.
14. Produce a control-versus-shadow report for every cluster, including
   nominal premium, signed/gross delta activity, coherence, baselines, Z,
   failed thresholds, provenance, and model versions.
15. Acceptance requires: five complete sessions and 30 shadow footprints for
   model disposition; exact replay equality; no persistence backpressure;
   no cross-symbol contamination; complete manifest; all accepted trades
   retained; and orders `0/0`. A zero footprint result is reported honestly,
   not treated as a pipeline failure.

