# Repository Instructions

These instructions apply to the entire repository.

## Required session start

Before changing the repository:

1. Determine the latest sequential session ID in `docs/agent/JOURNAL_INDEX.md`.
2. Read this file, `docs/agent/CURRENT_STATE.md`,
   `docs/agent/JOURNAL_INDEX.md`, the most recent session record, and any more
   specific instructions in the files being changed.
3. Compare the documented branch, commit, PR, and phase with live GitHub.
4. Report discrepancies before editing.
5. Create the next timestamped prompt and session records. Do not rely only on
   conversational memory.

Follow the permanent protocol in `docs/agent/README.md`. Completed prompt and
session records are append-only. Never rewrite or delete them; record corrections
as a new timestamped entry.

## Development rules

- GitHub is authoritative. Never commit directly to the default branch.
- Use focused branches and intentional commits. Open one draft PR per major phase.
- Do not merge without explicit owner approval.
- Do not rewrite or remove unrelated work.
- Keep provider SDKs behind ports; domain and signal code must not depend on vendor
  clients, databases, caches, web frameworks, or transport objects.
- Preserve deterministic behavior: identical ordered events and configuration must
  produce identical decisions.
- Use UTC internally and retain source, received, and normalized timestamps with
  provenance.
- Keep thresholds configuration-driven and versioned.
- Default trading to disabled. Alpaca paper trading is the only authorized
  execution environment. Live trading is prohibited.
- Never provision a paid Vast.ai resource without presenting candidates and
  receiving explicit approval.

## Secrets

- Never commit or log credentials, tokens, authorization headers, private keys,
  certificates, or populated `.env` files.
- Use variable names only in `.env.example`.
- Store runtime secrets only in an approved secret mechanism or a protected local
  `.env` with restrictive permissions.
- Redact sensitive values as `[REDACTED]`; report only presence and authentication
  success.

## Verification and session completion

Before ending a session:

1. Run all checks relevant to the change and retain meaningful evidence.
2. Run `git status`; confirm branch and commit SHA.
3. Confirm the draft PR state.
4. Complete the active session record and update `CURRENT_STATE.md`,
   `JOURNAL_INDEX.md`, and `NEXT_PROMPT.md`.
5. Commit and push journal updates to the active feature branch.
6. Confirm the journal files are visible on GitHub.
