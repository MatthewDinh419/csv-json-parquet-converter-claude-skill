# Parquet Schema Reference

## Always use pyarrow engine

Never use `fastparquet`. Use `engine="pyarrow"` for all reads and writes.

```python
df.to_parquet(path, engine="pyarrow")
pd.read_parquet(path, engine="pyarrow")
```

Reason: fastparquet has inconsistent type mapping, worse datetime handling, and no struct type support.

## Type mapping: CSV/JSON → Arrow

| Inferred type | Arrow type | Notes |
|---------------|-----------|-------|
| `int` | `pa.int64()` | Use int64, not int32, to avoid overflow |
| `float` | `pa.float64()` | |
| `string` | `pa.string()` (utf8) | Never use `pa.large_string()` unless >2GB column |
| `date` | `pa.timestamp("us")` | Microsecond precision is safe for most use cases |
| `bool` | `pa.bool_()` | |
| `null` / all-null | `pa.null()` | Warn user — downstream tools may reject null-typed columns |
| `leading_zeros` risk | `pa.string()` | Force string regardless of numeric appearance |
| `mixed_types` risk | `pa.string()` | Force string to avoid data loss |

## Categorical columns

Parquet stores categorical (dictionary-encoded) columns efficiently. When reading:

```python
# pyarrow DictionaryArray — decode before writing to CSV/JSON
chunked = table.column(col_name)
decoded = chunked.combine_chunks().dictionary_decode()
```

When writing from pandas Categorical:
```python
# pandas Categorical → Arrow dictionary automatically via Table.from_pandas
# The resulting Parquet column uses PLAIN_DICTIONARY encoding
```

## Decimal types

Decimal columns (`pa.decimal128(precision, scale)`) must never be converted to float — this loses precision.

**Reading Parquet with decimals:**
- For CSV output: convert to string (`str(value)`) to preserve all decimal digits
- For JSON output: same — use string representation
- For Parquet-to-Parquet: preserve the exact `decimal128(p, s)` type

**Writing decimals from CSV/JSON:**
- If source is a string like `"1234.56"`, cast to `decimal128` explicitly:
  ```python
  pa.array(["1234.56", "89.99"]).cast(pa.decimal128(10, 2))
  ```

## Datetime precision

| Parquet unit | Precision | CSV/JSON equivalent |
|-------------|-----------|---------------------|
| `ns` (nanosecond) | 1ns | No standard equivalent; millisecond at best |
| `us` (microsecond) | 1μs | ISO 8601 with 6 decimal places |
| `ms` (millisecond) | 1ms | ISO 8601 with 3 decimal places |
| `s` (second) | 1s | ISO 8601 |

When writing nanosecond timestamps to CSV/JSON, truncation occurs. Always warn:
`"Column '{name}' has nanosecond precision; output has microsecond precision."`

## Struct (nested) columns

Parquet supports nested struct columns natively via Arrow's struct type:
```python
pa.struct([pa.field("city", pa.string()), pa.field("state", pa.string())])
```

**Compatibility warning:** Include when outputting Parquet with structs:
> "Nested columns preserved as Arrow struct types. Spark ≥3.0, DuckDB, and Polars handle these correctly. Pandas <1.5, older Spark, and many BI tools may not."

**Flattening for CSV:** Use dot notation recursively. `address.city`, `address.coords.lat`.

## Compression

| Codec | Ratio | Speed | Use when |
|-------|-------|-------|----------|
| `snappy` | Medium | Fast | Default for operational data, repeated reads |
| `zstd` | High | Medium | Archival, infrequently read, storage cost matters |
| `gzip` | High | Slow | Broad compatibility (Hive, older Spark) |
| `none` | 1.0x | Fastest | In-memory workloads, tiny files |

**Default:** `snappy`. Switch to `zstd` when user mentions "archive", "store", or "cold storage".

## Schema preservation across round-trips

| Source type | → Parquet → | Back to source |
|-------------|------------|----------------|
| `string` with leading zeros | `pa.string()` | Preserved exactly |
| `decimal128` | `pa.decimal128(p, s)` | Preserved exactly |
| `timestamp[ns]` | `pa.timestamp("ns")` | Truncated to μs in CSV/JSON |
| `dict` encoded | `pa.dictionary(...)` | Decoded to values |
| `struct` | `pa.struct(...)` | Flattened with dot notation for CSV |

## Row group sizing

Default row group size (128MB) is appropriate for most conversions. Do not override unless user has a specific query engine requirement.
