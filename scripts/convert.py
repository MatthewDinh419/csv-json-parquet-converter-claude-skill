#!/usr/bin/env python3
"""
Phase 3+4: Convert a data file based on an inspect report.
Never relies on pandas defaults — all parameters are explicit.

Usage:
    python convert.py <input> <output> --inspect <report.json>
                      [--null-tolerance 0.05]
                      [--compression snappy|zstd]
                      [--json-orient records|ndjson]
                      [--flatten dot|skip]
                      [--no-infer-types]
"""

import argparse
import json
import os
import re
import sys
import tempfile

EUROPEAN_NUMBER_RE = re.compile(r"^\d{1,3}(\.\d{3})+(,\d+)?$")
THOUSANDS_RE = re.compile(r"^\$?-?\d{1,3}(,\d{3})+(\.\d+)?$")
CURRENCY_RE = re.compile(r"^[\$€£¥₹]")

NULL_VALUES = {"n/a", "na", "null", "none", "nil", "nan", "-", "--", "?",
               "unknown", "missing", "#n/a", "#null!", "not available", "not applicable"}


def _clean_numeric(value: str) -> str:
    v = value.strip()
    if CURRENCY_RE.match(v):
        v = v[1:]
    if EUROPEAN_NUMBER_RE.match(v):
        v = v.replace(".", "").replace(",", ".")
    else:
        v = v.replace(",", "")
    return v


def _build_na_values(report: dict) -> list:
    base = list(NULL_VALUES)
    extra = report.get("null_representations", [])
    combined = list(set(base) | set(extra or []))
    return combined


def _build_dtype_overrides(report: dict) -> dict:
    overrides = {}
    for col in report.get("columns", []):
        risks = col.get("risks", [])
        name = col["name"]
        if "leading_zeros" in risks:
            overrides[name] = str
        elif "mixed_types" in risks:
            overrides[name] = str
    return overrides


def _infer_parquet_schema(report: dict):
    import pyarrow as pa
    fields = []
    for col in report.get("columns", []):
        name = col["name"]
        risks = col.get("risks", [])
        itype = col.get("inferred_type", "string")

        if "leading_zeros" in risks or "mixed_types" in risks:
            arrow_type = pa.string()
        elif itype == "int":
            arrow_type = pa.int64()
        elif itype == "float" and "european_number_format" not in risks:
            arrow_type = pa.float64()
        elif itype == "date":
            arrow_type = pa.timestamp("us")
        else:
            arrow_type = pa.string()

        fields.append(pa.field(name, arrow_type))
    return pa.schema(fields) if fields else None


def csv_to_json(input_path: str, output_path: str, report: dict, orient: str, encoding: str,
                delimiter: str, has_header: bool, null_tolerance: float) -> dict:
    import pandas as pd

    na_values = _build_na_values(report)
    dtype_overrides = _build_dtype_overrides(report)
    size_bytes = report.get("size_bytes", 0)

    # Auto-select orient based on file size
    if orient == "auto":
        orient = "ndjson" if size_bytes > 100 * 1024 * 1024 else "records"

    df = pd.read_csv(
        input_path,
        encoding=encoding,
        sep=delimiter,
        header=0 if has_header else None,
        na_values=na_values,
        keep_default_na=False,
        dtype=dtype_overrides,
        low_memory=False,
    )

    # Post-process numeric columns with thousands separators or European format
    for col in report.get("columns", []):
        name = col["name"]
        risks = col.get("risks", [])
        if name not in df.columns:
            continue
        if "thousands_separator" in risks or "currency_symbol" in risks or "european_number_format" in risks:
            df[name] = df[name].apply(
                lambda v: _clean_numeric(str(v)) if pd.notna(v) and str(v).strip() else v
            )
            df[name] = pd.to_numeric(df[name], errors="coerce")

    rows_read = len(df)

    if orient == "ndjson":
        df.to_json(output_path, orient="records", lines=True, date_format="iso", force_ascii=False)
    else:
        df.to_json(output_path, orient="records", indent=2, date_format="iso", force_ascii=False)

    return {"rows_written": rows_read, "orient": orient, "output": output_path}


def csv_to_parquet(input_path: str, output_path: str, report: dict, encoding: str,
                   delimiter: str, has_header: bool, compression: str) -> dict:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    na_values = _build_na_values(report)
    dtype_overrides = _build_dtype_overrides(report)

    df = pd.read_csv(
        input_path,
        encoding=encoding,
        sep=delimiter,
        header=0 if has_header else None,
        na_values=na_values,
        keep_default_na=False,
        dtype=dtype_overrides,
        low_memory=False,
    )

    # Post-process numeric columns
    for col in report.get("columns", []):
        name = col["name"]
        risks = col.get("risks", [])
        if name not in df.columns:
            continue
        if "thousands_separator" in risks or "currency_symbol" in risks or "european_number_format" in risks:
            df[name] = df[name].apply(
                lambda v: _clean_numeric(str(v)) if pd.notna(v) and str(v).strip() else v
            )
            df[name] = pd.to_numeric(df[name], errors="coerce")
        elif col.get("inferred_type") in ("int", "float") and "leading_zeros" not in risks:
            converted = pd.to_numeric(df[name], errors="coerce")
            if converted.notna().sum() >= df[name].notna().sum() * 0.9:
                df[name] = converted
        elif col.get("inferred_type") == "date":
            df[name] = pd.to_datetime(df[name], errors="coerce", infer_datetime_format=True)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, output_path, compression=compression)

    return {"rows_written": len(df), "compression": compression, "output": output_path}


def _flatten_dict(d: dict, prefix: str = "", sep: str = ".") -> dict:
    items = {}
    for k, v in d.items():
        new_key = f"{prefix}{sep}{k}" if prefix else k
        if isinstance(v, dict):
            items.update(_flatten_dict(v, new_key, sep))
        elif isinstance(v, list):
            items[new_key] = json.dumps(v)
        else:
            items[new_key] = v
    return items


def json_to_csv(input_path: str, output_path: str, report: dict, encoding: str) -> dict:
    import pandas as pd

    json_info = report.get("json", {})
    structure = json_info.get("structure", "array_of_objects")
    warnings = []

    if structure == "ndjson":
        df = pd.read_json(input_path, lines=True, encoding=encoding)
    elif structure == "object_of_objects":
        with open(input_path, "r", encoding=encoding) as f:
            data = json.load(f)
        df = pd.DataFrame.from_dict(data, orient="index")
        df.index.name = "_key"
        df.reset_index(inplace=True)
    else:
        with open(input_path, "r", encoding=encoding) as f:
            data = json.load(f)
        if isinstance(data, dict):
            data = [data]

        has_nested = json_info.get("has_nested", False)
        if has_nested:
            data = [_flatten_dict(r) if isinstance(r, dict) else r for r in data]
            warnings.append("Nested objects flattened with dot notation. Arrays encoded as JSON strings.")

        df = pd.json_normalize(data)

    # Warn about any remaining list columns
    list_cols = [c for c in df.columns if df[c].apply(lambda v: isinstance(v, list)).any()]
    for col in list_cols:
        df[col] = df[col].apply(lambda v: json.dumps(v) if isinstance(v, list) else v)
        warnings.append(f"Column '{col}' contains arrays; encoded as JSON strings.")

    df.to_csv(output_path, index=False, encoding="utf-8")
    return {"rows_written": len(df), "warnings": warnings, "output": output_path}


def json_to_parquet(input_path: str, output_path: str, report: dict, encoding: str,
                    compression: str) -> dict:
    import pyarrow as pa
    import pyarrow.parquet as pq

    json_info = report.get("json", {})
    structure = json_info.get("structure", "array_of_objects")
    warnings = []

    if structure == "ndjson":
        import pyarrow.json as paj
        table = paj.read_json(input_path)
    else:
        with open(input_path, "r", encoding=encoding) as f:
            data = json.load(f)

        if isinstance(data, dict):
            if structure == "object_of_objects":
                rows = [{"_key": k, **v} for k, v in data.items()]
            else:
                rows = [data]
        else:
            rows = data

        has_nested = json_info.get("has_nested", False)
        if has_nested:
            warnings.append(
                "Nested JSON preserved as Arrow struct types. "
                "Some downstream tools (Spark <3.x, older BI tools) may not handle struct columns. "
                "Pass --flatten dot to flatten instead."
            )

        import pyarrow.json as paj
        ndjson_bytes = "\n".join(json.dumps(r) for r in rows).encode("utf-8")
        import io
        table = paj.read_json(io.BytesIO(ndjson_bytes))

    pq.write_table(table, output_path, compression=compression)
    return {"rows_written": table.num_rows, "warnings": warnings, "compression": compression, "output": output_path}


def parquet_to_csv(input_path: str, output_path: str) -> dict:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd

    table = pq.read_table(input_path)
    warnings = []

    # Convert categorical (dictionary) columns to their value types
    new_columns = {}
    for col_name in table.schema.names:
        field = table.schema.field(col_name)
        if pa.types.is_dictionary(field.type):
            chunked = table.column(col_name)
            decoded = chunked.combine_chunks().dictionary_decode()
            new_columns[col_name] = decoded

    if new_columns:
        for name, arr in new_columns.items():
            idx = table.schema.get_field_index(name)
            table = table.set_column(idx, name, arr)

    # Check for precision-sensitive types
    for field in table.schema:
        if pa.types.is_timestamp(field.type) and field.type.unit == "ns":
            warnings.append(
                f"Column '{field.name}' has nanosecond precision; CSV output has microsecond precision. "
                "Nanosecond digits will be truncated."
            )
        if pa.types.is_decimal(field.type):
            warnings.append(
                f"Column '{field.name}' is Decimal({field.type.precision},{field.type.scale}); "
                "written as string to preserve precision."
            )
        if pa.types.is_struct(field.type):
            warnings.append(
                f"Column '{field.name}' is a struct; flattened with dot notation in CSV output."
            )

    df = table.to_pandas()

    # Flatten any struct columns
    struct_cols = [f.name for f in table.schema if pa.types.is_struct(f.type)]
    for col in struct_cols:
        flat = pd.json_normalize(df[col].tolist())
        flat.columns = [f"{col}.{c}" for c in flat.columns]
        df = df.drop(columns=[col]).join(flat)

    # Write decimal as string to preserve precision
    for field in table.schema:
        if pa.types.is_decimal(field.type) and field.name in df.columns:
            df[field.name] = df[field.name].astype(str)

    df.to_csv(output_path, index=False, encoding="utf-8")
    return {"rows_written": len(df), "warnings": warnings, "output": output_path}


def parquet_to_json(input_path: str, output_path: str, orient: str) -> dict:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd

    table = pq.read_table(input_path)
    warnings = []

    # Decode dictionary columns
    new_columns = {}
    for col_name in table.schema.names:
        field = table.schema.field(col_name)
        if pa.types.is_dictionary(field.type):
            decoded = table.column(col_name).combine_chunks().dictionary_decode()
            new_columns[col_name] = decoded
    for name, arr in new_columns.items():
        idx = table.schema.get_field_index(name)
        table = table.set_column(idx, name, arr)

    for field in table.schema:
        if pa.types.is_timestamp(field.type) and field.type.unit == "ns":
            warnings.append(
                f"Column '{field.name}' has nanosecond precision; JSON output has microsecond precision."
            )
        if pa.types.is_struct(field.type):
            warnings.append(f"Column '{field.name}' is a nested struct; preserved as nested JSON object.")

    df = table.to_pandas()

    # Handle decimal columns
    for field in table.schema:
        if pa.types.is_decimal(field.type) and field.name in df.columns:
            df[field.name] = df[field.name].apply(lambda v: str(v) if v is not None else None)

    if orient == "ndjson":
        df.to_json(output_path, orient="records", lines=True, date_format="iso", force_ascii=False)
    else:
        df.to_json(output_path, orient="records", indent=2, date_format="iso", force_ascii=False)

    return {"rows_written": len(df), "warnings": warnings, "orient": orient, "output": output_path}


def convert(input_path: str, output_path: str, report: dict,
            null_tolerance: float, compression: str, json_orient: str) -> dict:
    in_fmt = report.get("format", "csv")
    out_ext = os.path.splitext(output_path.lower())[1].lstrip(".")
    out_fmt = {"csv": "csv", "json": "json", "ndjson": "json", "jsonl": "json", "parquet": "parquet"}.get(out_ext, out_ext)

    encoding_info = report.get("encoding", {})
    encoding = encoding_info.get("detected", "utf-8") or "utf-8"
    bom = encoding_info.get("bom", False)
    if bom:
        encoding = "utf-8-sig"

    delimiter = report.get("delimiter", ",")
    has_header = report.get("has_header", True)

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    if in_fmt == "csv" and out_fmt == "json":
        return csv_to_json(input_path, output_path, report, json_orient, encoding, delimiter, has_header, null_tolerance)
    elif in_fmt == "csv" and out_fmt == "parquet":
        return csv_to_parquet(input_path, output_path, report, encoding, delimiter, has_header, compression)
    elif in_fmt == "json" and out_fmt == "csv":
        return json_to_csv(input_path, output_path, report, encoding)
    elif in_fmt == "json" and out_fmt == "parquet":
        return json_to_parquet(input_path, output_path, report, encoding, compression)
    elif in_fmt == "parquet" and out_fmt == "csv":
        return parquet_to_csv(input_path, output_path)
    elif in_fmt == "parquet" and out_fmt == "json":
        return parquet_to_json(input_path, output_path, json_orient if json_orient != "auto" else "records")
    else:
        return {"error": f"Unsupported conversion: {in_fmt} → {out_fmt}"}


def main():
    parser = argparse.ArgumentParser(description="Convert a data file using an inspect report.")
    parser.add_argument("input", help="Input file path")
    parser.add_argument("output", help="Output file path")
    parser.add_argument("--inspect", required=True, help="Path to JSON report from inspect.py")
    parser.add_argument("--null-tolerance", type=float, default=0.05)
    parser.add_argument("--compression", default="snappy", choices=["snappy", "zstd", "gzip", "none"])
    parser.add_argument("--json-orient", default="auto", choices=["auto", "records", "ndjson"])
    args = parser.parse_args()

    with open(args.inspect) as f:
        report = json.load(f)

    result = convert(
        args.input, args.output, report,
        null_tolerance=args.null_tolerance,
        compression=args.compression if args.compression != "none" else None,
        json_orient=args.json_orient,
    )

    if "error" in result:
        print(json.dumps(result, indent=2))
        sys.exit(1)

    print(json.dumps(result, indent=2, default=str))
    sys.exit(0)


if __name__ == "__main__":
    main()
