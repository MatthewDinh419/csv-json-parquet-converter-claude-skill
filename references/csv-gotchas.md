# CSV Gotchas Reference

## Encoding

| Encoding | Common source | Symptoms | Fix |
|----------|--------------|----------|-----|
| UTF-8 with BOM | Excel "Save as CSV" on Windows | First column header has invisible `﻿` prefix | `encoding="utf-8-sig"` |
| Windows-1252 (cp1252) | Excel, Windows apps | `é`, `ü`, `ñ` become garbled; `â€™` for `'` | `encoding="cp1252"` |
| ISO-8859-1 (latin-1) | Legacy systems, European exports | Similar to cp1252; subtle differences above 0x9F | `encoding="iso-8859-1"` |
| UTF-16 | Some Windows tools | File starts with `ÿþ` or `þÿ` (BOM) | `encoding="utf-16"` |

**Detection rule:** Run chardet on first 64KB. If confidence < 0.7, try cp1252 and utf-8 and compare readability.

**BOM markers:** Strip before detection. pandas handles UTF-8 BOM via `encoding="utf-8-sig"`.

## Delimiters

| Delimiter | Use case | Detection pitfall |
|-----------|----------|-------------------|
| `,` | English/US default | Values with commas must be quoted; unquoted = wrong column count |
| `;` | European CSVs, German Excel | Easy to mistake for comma if file is ASCII-only |
| `\t` | TSV, database exports | Tab in field value (quoted) is valid; don't strip quotes |
| `\|` | Legacy data warehouses | Rare in wild; pipe in strings must be quoted |

**Sniffer validation:** After sniffing, count columns in first 50 rows. If variance > 2, the detected delimiter is wrong. Fall back to comma.

**Single-column files:** csv.Sniffer raises an exception. Catch it and default to comma with 1-column interpretation.

## Type pitfalls

### Leading zeros

Columns at risk (by name): `zip`, `postal`, `phone`, `account`, `id`, `code`, `fips`, `ean`, `isbn`, `sku`, `upc`, `ssn`, `ein`, `npi`

Columns at risk (by value): all values same length AND all digits AND any start with `0`

**Fix:** Pass `dtype={col: str}` to `pd.read_csv()`. Never infer type for these columns.

### Thousands separators (US format)

Pattern: `$1,234.56`, `1,234`, `-2,345.00`

**Fix:** Strip `$` prefix, remove `,`, parse as float. Do NOT use `pd.read_csv(thousands=",")` — it breaks columns that contain actual commas.

### European number format

Pattern: `1.234,56` (period = thousands, comma = decimal)

**Fix:** Remove `.` as thousands separator, replace `,` with `.`, parse as float. `1.234,56` → `1234.56`

### Mixed nulls

Common representations: `""`, `"N/A"`, `"n/a"`, `"NA"`, `"NULL"`, `"null"`, `"None"`, `"-"`, `"--"`, `"?"`, `"unknown"`, `"missing"`, `"#N/A"`, `"#NULL!"`

**Fix:** Pass `na_values=[list of all found representations]` + `keep_default_na=False` to control exactly what becomes NaN.

### Mixed line endings

`\r\n` (CRLF) mixed with `\n` (LF): pass `newline=""` to `open()`, then let csv module handle it. Never do `f.read().splitlines()` — it treats both as line endings but misses CRLF normalization.

### Quoted multi-line fields

Fields containing `\n` inside double quotes are valid RFC 4180. pandas handles this correctly with default `quotechar='"'`. The issue: line count != row count. Never count rows by counting lines for CSVs with multi-line fields.

## Pandas read_csv anti-patterns

| Anti-pattern | Problem | Fix |
|-------------|---------|-----|
| `pd.read_csv(f)` | Guesses encoding (often wrong), uses Python's default locale | Always pass `encoding=` explicitly |
| `pd.read_csv(f, na_values=["N/A"])` | Only replaces "N/A", not other null representations | Build full list from inspection |
| `pd.read_csv(f, dtype={"zip": int})` | Crashes on leading zeros | Use `str`, never `int` for ID/code columns |
| `pd.read_csv(f, infer_datetime_format=True)` | Silently wrong on ambiguous dates | Parse dates manually with explicit format or leave as string |
| `pd.read_csv(f, low_memory=False)` without dtypes | Defers type inference to end; still wrong for leading zeros | Combine with explicit `dtype=` overrides |
