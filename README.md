# csv-json-parquet-converter-claude-skill

A Claude Code skill that converts data files between **CSV**, **JSON**, and **Parquet** correctly on the first attempt. It encodes the decisions a senior data engineer makes before writing conversion code — encoding detection, delimiter sniffing, dtype validation, null normalization, and structure-aware output — so Claude handles real-world data instead of silently corrupting it.

## The problem

Claude's default behavior for "convert this CSV to JSON" is a one-liner like `pd.read_csv(f).to_json(out, orient='records')`. This works for clean, UTF-8, comma-delimited files. It fails — often silently — on real-world data:

| Issue | Example | Naive result |
|-------|---------|--------------|
| Non-UTF-8 encoding | Excel cp1252 export with `é`, `ü` | `UnicodeDecodeError` or garbled text |
| Semicolon delimiter | European CSV | Single column named `"id;name;amount"` |
| Leading-zero IDs | Zip code `"02134"` | Integer `2134` |
| Thousands separators | `"$1,234.56"` | `NaN` or string |
| Mixed nulls | `"N/A"`, `"-"`, `"NULL"`, `""` | Some become strings, some become NaN |
| NDJSON input | One JSON object per line | `ValueError: Trailing data` |

**Benchmark: 100% skill-guided vs 36% naive pandas on 11 tricky real-world fixtures.**

## How it works

Every conversion runs the same 5-phase workflow:

```
Phase 1+2  →  inspect_data.py   Detect encoding, delimiter, types, risks
Phase 3+4  →  convert.py        Convert with explicit parameters (never defaults)
Phase 5    →  validate.py       Verify row count, null inflation, column types
```

Claude orchestrates these scripts via Bash tool calls, reports what was detected, and stops with a clear error if validation fails — rather than silently producing wrong output.

## Supported conversions

| From \ To | CSV | JSON | Parquet |
|-----------|-----|------|---------|
| **CSV**     | —   | ✓    | ✓       |
| **JSON**    | ✓   | —    | ✓       |
| **Parquet** | ✓   | ✓    | —       |

## Installation

Copy the skill into your Claude Code skills directory:

```bash
cp -r . ~/.claude/skills/csv-json-parquet-converter-claude-skill/
```

Install required Python packages:

```bash
pip3 install chardet pandas pyarrow
```

## Usage

Once installed, trigger the skill naturally in Claude Code:

```
convert ~/data/customers.csv to JSON
convert ~/data/sales.csv to ~/data/sales.parquet
convert ~/data/events.parquet to CSV
inspect ~/data/report.csv and tell me what you find
```

### Example output

```
Detected: ISO-8859-1 encoding, comma-delimited, 10 rows.
Risks found:
  • customer_id, zip_code, phone — leading zeros (preserved as strings)
  • revenue — dollar sign + thousands separator (stripped → float)
  • last_order_date — ambiguous MM/DD/YYYY (treated as US format)
  • email, notes — mixed nulls: N/A, -, NULL, n/a (normalized to null)

Converted 10 → 10 rows. Validation passed.
Output: ~/data/customers.json
```

## What gets detected and fixed

### Encoding
- Auto-detects encoding with `chardet` (first 64KB sample)
- Handles cp1252, ISO-8859-1, UTF-16, UTF-8 BOM
- Always outputs UTF-8

### Delimiters
- Sniffs with `csv.Sniffer`, validates by column count consistency across 50 rows
- Supports `,` `;` `\t` `|`

### Type safety
- **Leading zeros**: columns named `zip`, `postal`, `phone`, `account`, `id`, `code` + values that start with `0` → preserved as strings
- **Thousands separators**: `$1,234.56` → float `1234.56`
- **European numbers**: `1.234,56` → float `1234.56`
- **Mixed types**: columns with inconsistent types → preserved as strings

### Nulls
- Detects and normalizes: `""`, `"N/A"`, `"n/a"`, `"NA"`, `"NULL"`, `"null"`, `"None"`, `"-"`, `"--"`, `"?"`, `"unknown"`, `"missing"`

### JSON structures
- Detects: array of objects, NDJSON, object-of-objects, nested, inconsistent schemas
- Flattens nested objects with dot notation (`address.city`) for CSV output
- Preserves Arrow struct types for Parquet output

### Parquet
- Always uses `pyarrow` engine (never `fastparquet`)
- Decodes categorical columns to their underlying string values
- Preserves decimal precision as strings when writing to CSV/JSON
- Warns on nanosecond datetime precision loss

## Validation

After every conversion, the skill verifies:

- **Row count** matches exactly
- **Null inflation** — warns at 1% increase, fails at 5%+ (configurable via `--null-tolerance`)
- **Column types** — leading-zero columns confirmed as strings in output

## Project structure

```
csv-json-parquet-converter-claude-skill/
├── SKILL.md                     # Skill instructions and CLI contract
├── scripts/
│   ├── inspect_data.py          # Phase 1+2: inspection and risk detection
│   ├── convert.py               # Phase 3+4: strategy selection and conversion
│   └── validate.py              # Phase 5: post-conversion validation
├── references/
│   ├── csv-gotchas.md           # Encoding, delimiter, type pitfalls
│   ├── json-structures.md       # records, NDJSON, nested patterns
│   └── parquet-schemas.md       # Type mapping, compression, partitioning
└── tests/
    ├── fixtures/                # 27 tricky real-world test files
    │   └── README.md            # What each fixture tests
    └── test_conversions.py      # Benchmark harness
```

## Running the benchmark

```bash
# Skill-guided only
python3 tests/test_conversions.py --skill

# Naive pandas baseline only
python3 tests/test_conversions.py --baseline

# Full comparison with lift calculation
python3 tests/test_conversions.py --both
```

## Running scripts directly

```bash
# Inspect a file
python3 scripts/inspect_data.py data.csv

# Full pipeline
python3 scripts/inspect_data.py data.csv > /tmp/report.json
python3 scripts/convert.py data.csv data.parquet --inspect /tmp/report.json
python3 scripts/validate.py data.csv data.parquet --inspect /tmp/report.json
```

## Dependencies

- Python 3.9+
- `pandas >= 2.0`
- `pyarrow >= 12.0`
- `chardet >= 5.0`
