---
name: csv-json-parquet-converter-claude-skill
description: Convert data files between CSV, JSON, and Parquet formats with encoding detection, type safety, and validation. Use when the user asks to convert a CSV, JSON, or Parquet file, or mentions file extensions like .csv, .json, .parquet in a conversion context.
triggers:
  - convert csv
  - csv to json
  - csv to parquet
  - json to csv
  - json to parquet
  - parquet to csv
  - parquet to json
  - to parquet
  - to json
  - read this data file
  - convert this file
  - .csv
  - .parquet
  - .json
alwaysApply: false
---

# Data Format Converter

You are running a 5-phase conversion workflow that encodes the decisions a senior data engineer makes before writing conversion code. Never skip phases. Never use pandas defaults. All parameters must be explicit.

## Skill location

The skill scripts are at: `~/.claude/skills/csv-json-parquet-converter-claude-skill/scripts/`

- `inspect_data.py` — Phase 1+2
- `convert.py` — Phase 3+4
- `validate.py` — Phase 5

Reference docs at: `~/.claude/skills/csv-json-parquet-converter-claude-skill/references/`

## Phase 1+2: Inspect

Run inspection and save the report to a temp file:

```bash
python3 ~/.claude/skills/csv-json-parquet-converter-claude-skill/scripts/inspect_data.py <input_file> > /tmp/converter_report.json
```

Read `/tmp/converter_report.json`. Report what was detected to the user:
- Encoding (if not UTF-8)
- Delimiter (if not comma)
- Any risks detected (leading_zeros, thousands_separator, european_number_format, mixed_types, ambiguous_date_format, nested_json, inconsistent_schema, etc.)
- Null representations found

If the file cannot be read (exit code 1), report the error and stop.

## Phase 3+4: Convert

Run conversion using the saved report:

```bash
python3 ~/.claude/skills/csv-json-parquet-converter-claude-skill/scripts/convert.py \
  <input_file> <output_file> \
  --inspect /tmp/converter_report.json \
  [--compression snappy|zstd] \
  [--json-orient auto|records|ndjson] \
  [--null-tolerance 0.05]
```

**Default choices (never ask — apply and report):**
- JSON orient: `auto` (records for ≤100MB, ndjson for >100MB)
- Parquet compression: `snappy` (use `zstd` if user mentions archival)
- Null tolerance: `0.05` (5% inflation before failing)
- Date format ambiguity: ISO 8601 interpretation — document the assumption
- Compressed inputs (`.gz`, `.zst`): handled transparently, no special flags needed

## Phase 5: Validate

```bash
python3 ~/.claude/skills/csv-json-parquet-converter-claude-skill/scripts/validate.py \
  <input_file> <output_file> \
  --inspect /tmp/converter_report.json
```

Read the validation JSON. Report to the user:
- Row count match/mismatch
- Any null inflation warnings or failures
- Column type checks (leading-zero columns confirmed as strings, etc.)
- Any warnings about precision loss (datetime nanoseconds, decimal → string)

**If validation fails:** Report the specific issue. Do NOT silently retry with different parameters. Stop and ask the user how to proceed.

## Verbosity rules

- **File < 10MB or no risks detected**: terse — one-line summary of what was detected and converted.
- **File ≥ 10MB or risks detected**: detailed — list each risk, what was done about it, validation results.
- User can always override with `--verbose` or `--quiet` in their request.

## Dependency check

Before running the workflow, verify required packages are available:

```bash
python3 -c "import chardet, pandas, pyarrow; print('ok')" 2>&1
```

If any package is missing, install it:
```bash
pip3 install chardet pandas pyarrow --quiet
```

## CLI contract

### inspect_data.py

**Input:** `<file_path> [--sample-rows N]`
**Output:** JSON to stdout
**Exit codes:** 0=success, 1=unreadable or empty file

Key output fields:
- `format`: `"csv"` | `"json"` | `"parquet"`
- `encoding.detected`: encoding string (e.g. `"cp1252"`, `"utf-8"`)
- `encoding.bom`: boolean — file has BOM marker
- `delimiter`: delimiter character for CSV files
- `has_header`: boolean
- `row_count`: approximate row count
- `columns[].name`: column name
- `columns[].inferred_type`: `"int"` | `"float"` | `"string"` | `"date"` | `"bool"` | `"null"`
- `columns[].risks`: list of risk flags (see below)
- `null_representations`: list of null strings found
- `risks`: union of all column risks plus file-level risks
- `json.structure`: `"array_of_objects"` | `"ndjson"` | `"object_of_objects"` | `"single_object"`
- `json.has_nested`: boolean
- `json.consistent_schema`: boolean
- `parquet.columns[].arrow_type`: Arrow type string
- `parquet.columns[].risks`: list

**Risk flags:**
- `leading_zeros` — column contains or likely contains leading-zero strings (zip codes, IDs)
- `thousands_separator` — US-format numeric with commas (e.g. `1,234.56`)
- `european_number_format` — European-format numeric with period thousands, comma decimal (e.g. `1.234,56`)
- `currency_symbol` — values start with `$`, `€`, etc.
- `mixed_types` — column has inconsistent types across rows
- `ambiguous_date_format` — date format could be MM/DD or DD/MM
- `mixed_null_representations` — multiple null representations in one column
- `non_utf8_encoding` — file is not UTF-8
- `bom_marker` — file has BOM prefix
- `all_null` — column is entirely null
- `nested_json` — JSON has nested objects/arrays
- `inconsistent_schema` — JSON records have different keys
- `categorical` — Parquet column is dictionary-encoded
- `decimal_precision` — Parquet column is Decimal type
- `nested_struct` — Parquet column is a struct type
- `nanosecond_precision_loss` — Parquet timestamp has ns precision that CSV/JSON cannot represent

### convert.py

**Input:**
```
<input_path> <output_path>
--inspect <report_json_path>
[--null-tolerance FLOAT]     # default 0.05
[--compression snappy|zstd|gzip|none]  # default snappy
[--json-orient auto|records|ndjson]    # default auto
```

**Output:** JSON to stdout with `rows_written`, `warnings`, `output`
**Exit codes:** 0=success, 1=error

### validate.py

**Input:**
```
<input_path> <output_path>
--inspect <report_json_path>
[--null-tolerance FLOAT]     # default 0.05
```

**Output:** JSON to stdout with `passed`, `issues`, `warnings`, `checks`
**Exit codes:** 0=passed, 1=failed

## Example workflow

User: "convert data.csv to data.parquet"

```bash
# Phase 1+2
python3 ~/.claude/skills/csv-json-parquet-converter-claude-skill/scripts/inspect_data.py data.csv > /tmp/converter_report.json

# Report to user:
# "Detected: UTF-8, comma-delimited, 1204 rows.
#  Risks: zip_code column has leading zeros (will preserve as string)"

# Phase 3+4
python3 ~/.claude/skills/csv-json-parquet-converter-claude-skill/scripts/convert.py \
  data.csv data.parquet --inspect /tmp/converter_report.json

# Phase 5
python3 ~/.claude/skills/csv-json-parquet-converter-claude-skill/scripts/validate.py \
  data.csv data.parquet --inspect /tmp/converter_report.json

# Report to user:
# "Converted 1204 rows. Validation passed.
#  zip_code preserved as string. Compression: snappy."
```
