---
ontology: true
type: spec
domain: lakefed-cursor
summary: Secret JSON contract, the three auth modes for a source database credential
tags: [contract, secret, auth, ntlm, entra-id, sqlserver]
related: [../research, ../data-model, source_adapter]
status: draft
discovered: 2026-10-25
verified: false
workspace: lakefed-cursor
---

# Contract: Secret JSON

One secret per source database in scope `lakefed-cursor`. The key is `control.secret_key`.
Function `source_io._extract_creds` parses it. `SourceAdapter.auth_options` turns it into JDBC
options.

## Shapes

| `auth_mode` | Required fields | Engine |
|---|---|---|
| absent or `sql` | `user`, `password` | all |
| `ntlm` | `user`, `password`, `domain` | `sqlserver` |
| `aad_sp` | `client_id`, `client_secret` | `sqlserver` |

```json
{"user": "svc_lakefed", "password": "..."}
{"auth_mode": "ntlm", "domain": "CORP", "user": "svc_lakefed", "password": "..."}
{"auth_mode": "aad_sp", "client_id": "...", "client_secret": "..."}
```

## Rules

1. A missing `auth_mode` means `sql`.
2. An unknown `auth_mode` fails with a message that lists `sql`, `ntlm`, `aad_sp`.
3. A missing required field fails before any connect attempt. The message names the field.
4. `ntlm` or `aad_sp` on a source that is not `sqlserver` fails at plan time.
5. No log line, error, or telemetry row contains `password` or `client_secret` values.
6. A `remote_query` row does not read the secret.

## Tests

`tests/source_io_test.py` covers one case per row of the table above, plus the unknown mode and
the missing field. `tests/source_adapters_test.py` covers `auth_options` per mode for
`SqlServerAdapter` and the raise for `OracleAdapter`.
