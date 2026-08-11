# Work Package 1 correction handoff

The corrected certification is executable and deterministic. It consumes the
accelerated RTH scenario, invokes the production signal composition boundary
with provider, clock, and persistence test ports, runs real coverage planning,
persists and replays executed inputs, and derives invariant status from trace
records or inspected state.

The production Phase 4 runner still lacks the required deterministic
composition interface. The certificate therefore records that missing
interface as an observed failed invariant; production planner behavior was not
changed. Providers were not run, trading stayed disabled, and orders remained
`0/0`.

Runtime artifacts:

- `/tmp/phase4b-certification/CERTIFICATE.json`
- `/tmp/phase4b-certification-terminal.json`
- `/tmp/phase4b-wp1-handoff.tar.gz`

The committed example fixture is not exact-head runtime evidence.
