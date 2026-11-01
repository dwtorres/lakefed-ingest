---
ontology: true
type: spec
domain: lakefed-cursor
summary: Checkpoint position JSON contract for cursor tables
tags: [contract, checkpoint, position, cursor, json]
related: [../data-model, control_row]
status: draft
discovered: 2026-10-25
verified: false
workspace: lakefed-cursor
---

# Contract: Position JSON

Column `lakefed_cursor.checkpoint.position`. Functions `cursor_engine.encode_position` and
`cursor_engine.decode_position`.

## Shape

```json
{"expr": "GREATEST(COALESCE(UPDATED_AT, DATE '1900-01-01'), COALESCE(CREATED_AT, DATE '1900-01-01'))",
 "kind": "db_timestamp",
 "type": "timestamp(6)",
 "value": "2026-10-25T14:03:11.123456"}
```

| Field | Type | Rule |
|---|---|---|
| `expr` | string | the control row `cursor_expression` text at the last seed. Compared byte for byte |
| `kind` | string | the control row `cursor_kind` at the last seed |
| `type` | string | the source type name of the expression result |
| `value` | string or null | numeric as digits, temporal as ISO 8601, rowversion as a decimal integer. Null after a seed of an empty table |

## Rules

1. `decode_position` returns `None` for a missing `expr`, `kind`, or `type`.
2. A position with `expr` different from the control row triggers a reseed.
3. A position with `kind` different from the control row triggers a reseed.
4. `value` for a `datetime` or `smalldatetime` type carries at most three fractional digits.
5. The position never moves backwards. If `hi` is below `value`, `plan_increment` raises.
6. The old shape `{col, type, value}` is not accepted.
