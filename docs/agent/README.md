# Codex Operating Journal

This directory is the append-only operating record for Codex work on the
Institutional Signal Engine.

## Required files

- `CURRENT_STATE.md` is the authoritative project snapshot.
- `JOURNAL_INDEX.md` lists every session in chronological order.
- `NEXT_PROMPT.md` contains the complete proposed instruction for the next session.
- `prompts/` contains immutable prompt records.
- `sessions/` contains immutable chronological session records.

## Session protocol

1. Read `AGENTS.md`, `CURRENT_STATE.md`, `JOURNAL_INDEX.md`, the latest session
   record, and all applicable repository instructions.
2. Compare the documented state with live GitHub before modifying the repository.
3. Allocate the next `SESSION-NNNN` identifier and use a UTC timestamp formatted
   as `YYYYMMDDTHHMMSSZ` in its filenames.
4. Create both the prompt and session record before implementation begins.
5. Append evidence chronologically. Never rewrite or delete a completed session
   record. Add corrections as new timestamped entries.
6. Redact credentials, tokens, authorization headers, private keys, and `.env`
   values as `[REDACTED]`.
7. Before ending, run relevant checks and `git status`; verify the branch, commit,
   and draft PR; update the state, index, next prompt, and session record; commit
   and push those updates; and confirm their visibility on GitHub.

When output is excessive, record the exact command, relevant output, exit status,
the full artifact location if one exists, and a digest of omitted repetition.
