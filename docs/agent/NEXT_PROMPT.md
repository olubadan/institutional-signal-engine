# Proposed Next Codex Prompt

```text
Review Phase 2 draft pull request #2 for olubadan/institutional-signal-engine as SESSION-0009.

Before taking any action, read AGENTS.md, docs/agent/CURRENT_STATE.md, docs/agent/JOURNAL_INDEX.md, the most recent session record at docs/agent/sessions/20260803T190342Z-SESSION-0008.md, docs/environment/VAST_SETUP.md, docs/infrastructure/VAST_INSTANCE.md, docs/adr/0001-vast-ubuntu-22-exception.md, docs/07_Requirements_Traceability_Matrix.md, and all applicable repository instructions. Compare documented state with live GitHub and report discrepancies before changing anything.

Review the complete PR #2 diff and Phase 2 acceptance evidence. Verify read-only that instance 46725769 remains SSH-accessible, trading remains disabled, the repository checkout is on chore/vast-environment-bootstrap, Docker and institutional-signal-dependencies.service are enabled/active, PostgreSQL and Redis are healthy, protected environment files retain root:root mode 0600, and the documented make lint/typecheck/build/test commands pass. Never display credentials or the public SSH connection string.

Do not modify or merge PR #2. Do not request provider credentials, provision another instance, implement application/trading code, or enable trading. Report whether Phase 2 is ready to merge, the Ubuntu 22.04/finite-duration exceptions, remaining blockers, and the exact owner decision required. Create a journal record only if repository mutation becomes necessary; otherwise provide a read-only review.
```
