# Contributing

## Workflow

1. Start from an up-to-date default branch and create a focused feature branch.
2. Keep implementation, tests, documentation, and traceability changes together.
3. Run the baseline checks documented in `README.md`.
4. Use concise commits whose messages describe one intentional unit of work.
5. Push the branch and open a draft PR with acceptance evidence.
6. Do not merge without explicit owner approval.

## Definition of done

A change is complete only when:

- its behavior and boundaries are documented;
- its requirement IDs and evidence are updated where applicable;
- automated tests cover success, boundary, and failure behavior proportionate to
  risk;
- formatting, lint, typing, and tests pass;
- secrets and sensitive output are absent from the diff and logs;
- operational or migration effects are documented; and
- the Codex operating journal is complete and published.

See `docs/development/CONVENTIONS.md` for code-level rules.
