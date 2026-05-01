# Benchmark Fixtures

Each fixture targets a specific real-world data quality issue that naive pandas one-liners fail on.

## CSV Fixtures

| File | Tests | Expected Behavior |
|------|-------|-------------------|
| `01_clean_utf8.csv` | Baseline — clean UTF-8 comma CSV | Standard conversion, no special handling needed |
| `02_windows1252_accented.csv` | Encoding detection — cp1252 with accented chars | Detect cp1252, read with correct encoding; output readable UTF-8 |
| `03_semicolon_delimited.csv` | Delimiter detection — European semicolon CSV | Detect `;` delimiter; `amount` column preserved as string (has commas as thousands sep) |
| `04_tsv_tabs_in_fields.csv` | TSV with tab chars inside quoted fields | Detect tab delimiter; multi-word fields with embedded tabs preserved |
| `05_leading_zero_zips.csv` | Leading zeros — zip codes that pandas casts to int | `zip_code` column preserved as string `"01234"`, not integer `1234` |
| `06_mixed_nulls.csv` | Mixed null representations | All of `""`, `"N/A"`, `"-"`, `"NULL"`, `"n/a"` treated as null |
| `07_thousands_separators.csv` | Thousands separators and currency symbols | `price` column: strip `$` and `,`, parse as float |
| `08_ambiguous_dates.csv` | Ambiguous date format (MM/DD vs DD/MM) | Default to ISO interpretation; document assumption in output |
| `09_utf8_bom.csv` | UTF-8 BOM marker | Strip BOM; first column header is `id` not `﻿id` |
| `10_mixed_line_endings.csv` | Mixed CRLF + LF line endings | All rows read correctly regardless of line ending |
| `11_multiline_fields.csv` | Quoted multi-line fields | Fields containing `\n` inside quotes preserved intact |
| `12_single_column.csv` | Single column — delimiter detection failure case | Don't crash; detect no delimiter (single column); produce valid output |
| `13_empty.csv` | Empty file | Fail gracefully with descriptive error; exit code 1 |
| `14_headers_only.csv` | Headers-only file (zero data rows) | Produce empty output with correct schema; warn user |
| `15_wide_file.csv` | 50-column file | All 50 columns read and converted correctly |
| `16_mixed_types.csv` | Column with mixed int/float/string/bool/null types | Flag `value` as mixed-type; preserve as string to avoid data loss |
| `17_phone_account_ids.csv` | Phone numbers and account IDs with leading zeros | Both `account_id` and `phone` preserved as strings |
| `23_no_header.csv` | CSV without a header row | Detect missing header; auto-generate column names (`col_0`, `col_1`, ...) |

## JSON Fixtures

| File | Tests | Expected Behavior |
|------|-------|-------------------|
| `18_ndjson.json` | Line-delimited JSON (NDJSON) | Detect NDJSON format; use `pyarrow.json.read_json` for Parquet, line-by-line for CSV |
| `19_nested_json.json` | Deeply nested JSON (objects within objects) | Flatten to dot notation for CSV (`address.city`); preserve struct for Parquet |
| `20_inconsistent_schema.json` | Records with different keys | Union all keys; missing fields become null |
| `21_object_of_objects.json` | JSON object-of-objects (not array) | Detect format; treat outer keys as an index column |
| `22_json_with_arrays.json` | Records containing array fields | Array fields → JSON-encoded string in CSV, with warning; preserve as list type in Parquet |

## Parquet Fixtures

| File | Tests | Expected Behavior |
|------|-------|-------------------|
| `24_categorical_columns.parquet` | Categorical dtype columns | Write underlying string values, not category codes |
| `25_decimal_types.parquet` | Decimal128 precision columns | Preserve exact decimal precision; never fall back to float |
| `26_nested_struct.parquet` | Nested struct columns | Flatten with dot notation for CSV; warn about nesting when converting to JSON |
| `27_datetime_precision.parquet` | Nanosecond datetime precision | Document precision loss when writing to CSV/JSON (ms or s precision); never silently truncate |
