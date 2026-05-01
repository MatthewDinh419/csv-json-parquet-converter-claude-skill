# JSON Structure Reference

## Detection

| Structure | Description | How to detect |
|-----------|-------------|---------------|
| `array_of_objects` | `[{"id": 1, ...}, ...]` | File starts with `[`, each element is a dict |
| `ndjson` | One JSON object per line | First line parses as JSON, second line also parses as JSON |
| `object_of_objects` | `{"key1": {...}, "key2": {...}}` | Top-level is dict, all values are dicts |
| `single_object` | `{"field": "value", ...}` | Top-level is dict, values are not all dicts |
| `array_of_scalars` | `[1, 2, 3]` | Top-level is list, elements are not dicts |

**Detection order:** Try NDJSON first (two-line test). If that fails, parse full file and check type.

## NDJSON (newline-delimited JSON)

Each line is a complete, independent JSON object:
```
{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob"}
```

**Read for Parquet:** Use `pyarrow.json.read_json(path)` directly. Faster than pandas, better typing.
**Read for CSV:** Line-by-line parse with `json.loads()`, or `pd.read_json(path, lines=True)`.

## Inconsistent schemas

Records have different keys across rows:
```json
[
  {"id": 1, "email": "alice@example.com", "age": 30},
  {"id": 2, "phone": "555-1234"},
  {"id": 3, "email": "carol@example.com", "department": "Eng"}
]
```

**Strategy:** Take union of all keys. Missing fields become null. `pd.json_normalize(data)` handles this automatically.

## Nested JSON → CSV flattening

**Rule:** Flatten with dot notation. Arrays become JSON-encoded strings with a warning.

```python
# Input
{"id": 1, "address": {"city": "Boston", "state": "MA"}, "tags": ["a", "b"]}

# Output columns
{"id": 1, "address.city": "Boston", "address.state": "MA", "tags": "[\"a\", \"b\"]"}
```

**Multi-level nesting:** Apply recursively. `address.coords.lat` for three levels deep.

**Arrays:** Never silently drop. JSON-encode as string and warn the user. Example:
`tags: ["python", "data"]` → `tags: "[\"python\", \"data\"]"` + warning.

## Nested JSON → Parquet

Preserve nesting as Arrow struct types — do NOT flatten unless user asks.

```python
# Arrow struct type preserves structure
pa.struct([pa.field("city", pa.string()), pa.field("state", pa.string())])
```

**Warning to include:** "Nested columns preserved as Arrow struct types. Some downstream tools (Spark <3.x, older BI tools, pandas <1.5) may not handle struct columns. Pass `--flatten dot` to flatten instead."

## Object-of-objects → table

Treat outer keys as an index column:

```json
{"alice": {"age": 30, "city": "Boston"}, "bob": {"age": 25, "city": "Chicago"}}
```

Becomes:
```
_key,age,city
alice,30,Boston
bob,25,Chicago
```

## JSON → Parquet: prefer pyarrow.json

For NDJSON, `pyarrow.json.read_json()` is better than pandas:
- Handles GB-scale files
- Infers Arrow types natively
- Handles inconsistent schemas with `UnexpectedFieldBehavior`

For regular JSON arrays, serialize as NDJSON to a BytesIO buffer and use `pyarrow.json.read_json`.

## Output orient selection

| Orient | Format | Use when |
|--------|--------|----------|
| `records` | `[{"a": 1}, {"a": 2}]` | Default for files ≤ 100MB; most compatible |
| `ndjson` | `{"a": 1}\n{"a": 2}` | Files > 100MB; streaming processors; Spark |

Never use pandas `columns`, `index`, `split`, or `table` orients — they are not standard JSON and almost never what users want.
