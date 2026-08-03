# Secret Handling

## Rules

- Never commit credentials, tokens, authorization headers, private keys,
  certificates, populated `.env` files, or captured provider payloads containing
  account data.
- `.env.example` contains names only. It is not a runtime configuration.
- Runtime credentials belong in an approved secret manager or a protected `.env`
  on Vast with mode `0600`; that file remains ignored by Git.
- CI credentials, when justified, belong in GitHub Actions secrets and must not be
  available to untrusted pull-request code.
- Disable shell tracing around authentication. Sanitize saved commands and output.
- Logs report only whether a variable is present and whether authentication passed.
  They never show full or partial secret values.
- Replace sensitive values in the operating journal and durable artifacts with
  `[REDACTED]`.

## Credential names

The baseline reserves the names in `.env.example`. Before integration, adapters
must confirm their exact required variables and document where the owner should
enter them securely. Do not request credentials until they are needed.

## Incident response

If a secret may have entered Git or logs, stop work, do not reproduce it, identify
the affected location, notify the owner, and recommend revocation and rotation.
History rewriting or artifact deletion requires explicit approval.
