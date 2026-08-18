# Phase 4B live observational-session run card

This is an executable evidence run card. It does not assert provider or host
readiness; those are preflight observations recorded by the operator. Trading
must remain disabled and orders must remain `0/0`. Do not use the hermetic
`phase4b_certify` command for a live-session bundle.

## Immutable head and environment validation

Run from the checked-out repository and substitute the exact approved head:

```sh
EXPECTED_HEAD=3caa6dfffe3b6b08ffdb89aeae2d0ed46e61a294
test "$(git rev-parse HEAD)" = "${EXPECTED_HEAD}"
git status --short
gh pr view 6 --json state,isDraft,headRefOid,mergeable
RUNTIME_ENV_FILE=/etc/institutional-signal-engine/runtime.env \
  uv run python -m institutional_signal_engine.live_session validate-environment \
  --session-output "/var/lib/institutional-signal-engine/sessions/${EXPECTED_HEAD}-$(date -u +%Y%m%dT%H%M%SZ)" \
  --expected-git-commit "${EXPECTED_HEAD}" \
  --repository-root /opt/institutional-signal-engine
```

The protected runtime must explicitly configure `DATABASE_URL`,
`LIVE_JOURNAL_DIRECTORY`, and `LIVE_EVIDENCE_AUTHORITY_KEY_FILE`. A missing or
unavailable durable repository fails before any provider connection.

## Full-session launch

Choose a valid weekday market date. The output directory must not exist and is
never redirected over an earlier artifact:

```sh
SESSION_OUTPUT="/var/lib/institutional-signal-engine/sessions/${EXPECTED_HEAD}-$(date -u +%Y%m%dT%H%M%SZ)"
BOOTSTRAP_CHECKPOINT_DIR="/var/lib/institutional-signal-engine/evidence/r1-bootstrap-v8-full"
RUNTIME_ENV_FILE=/etc/institutional-signal-engine/runtime.env \
  uv run python -m institutional_signal_engine.live_session run \
  --session-output "${SESSION_OUTPUT}" \
  --market-date 2026-08-11 \
  --expected-git-commit "${EXPECTED_HEAD}" \
  --bootstrap-checkpoint-dir "${BOOTSTRAP_CHECKPOINT_DIR}" \
  --repository-root /opt/institutional-signal-engine
```

The checkpoint directory must contain valid, hashed per-symbol checkpoints for
the complete candidate universe. The live observer loads these checkpoints and
does not refetch historical bars through the monolithic bootstrap path.

There is one production mode. The session loop targets 09:30–16:00
America/New_York and stops intake at 16:00; it is not a nominal seconds run.

## Health observation and fail-closed termination

Observe only the staged directory and process state; do not print the runtime
file or credentials:

```sh
test -f "${SESSION_OUTPUT}/INCOMPLETE" && echo INCOMPLETE || echo COMPLETE
pgrep -af 'institutional_signal_engine.live_session run' || true
```

On provider, acknowledgement, clock, persistence, or shutdown failure, retain
the directory as incomplete and stop the process. Never create or copy a
manifest or certificate to make a partial run appear complete.

## Artifact inspection, replay, and certification

```sh
RUNTIME_ENV_FILE=/etc/institutional-signal-engine/runtime.env \
  uv run python -m institutional_signal_engine.live_session inspect \
  --bundle "${SESSION_OUTPUT}" \
  --repository-root /opt/institutional-signal-engine

RUNTIME_ENV_FILE=/etc/institutional-signal-engine/runtime.env \
  uv run python -m institutional_signal_engine.live_session replay \
  --bundle "${SESSION_OUTPUT}" \
  --output "${SESSION_OUTPUT}.replay.json" \
  --repository-root /opt/institutional-signal-engine

RUNTIME_ENV_FILE=/etc/institutional-signal-engine/runtime.env \
  uv run python -m institutional_signal_engine.live_session certify \
  --bundle "${SESSION_OUTPUT}" \
  --replay-output "${SESSION_OUTPUT}.certification-replay.json" \
  --certificate-output "${SESSION_OUTPUT}.LIVE_CERTIFICATE.json" \
  --repository-root /opt/institutional-signal-engine
```

The live certificate states that one persisted observational session was
validated offline. The separate hermetic acceptance certificate remains
available only through its existing command and retains its original meaning.
