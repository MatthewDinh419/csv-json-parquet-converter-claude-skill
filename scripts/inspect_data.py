#!/usr/bin/env python3
"""
Phase 1+2: Inspect a data file and detect encoding, delimiter, types, and risks.
Outputs a JSON report to stdout. Exit code 0 on success, 1 on unreadable file.

Usage:
    python inspect.py <input_file> [--sample-rows 100]
"""

import argparse
import csv
import io
import json
import os
import re
import sys

LEADING_ZERO_NAME_PATTERN = re.compile(
    r"(zip|postal|phone|account|id|code|fips|ean|isbn|sku|upc|ssn|ein|npi)",
    re.IGNORECASE,
)

NULL_REPRESENTATIONS = {
    "n/a", "na", "null", "none", "nil", "nan", "-", "--", "?", "unknown",
    "missing", "#n/a", "#null!", "not available", "not applicable",
}

THOUSANDS_PATTERN = re.compile(r"^\$?-?\d{1,3}(,\d{3})+(\.\d+)?$")
EUROPEAN_NUMBER_PATTERN = re.compile(r"^\d{1,3}(\.\d{3})+(,\d+)?$")
CURRENCY_PATTERN = re.compile(r"^[\$€£¥₹]")


def detect_format(path: str) -> str:
    ext = os.path.splitext(path.lower())[1].lstrip(".")
    if ext in ("csv", "tsv"):
        return "csv"
    if ext in ("json", "ndjson", "jsonl"):
        return "json"
    if ext == "parquet":
        return "parquet"
    # Try content sniff for ambiguous extensions
    try:
        with open(path, "rb") as f:
            header = f.read(4)
        if header[:4] == b"PAR1":
            return "parquet"
        text = header.decode("utf-8", errors="replace")
        stripped = text.lstrip()
        if stripped.startswith(("{", "[")):
            return "json"
    except Exception:
        pass
    return "csv"


def detect_encoding(path: str) -> dict:
    try:
        import chardet
    except ImportError:
        return {"detected": "utf-8", "confidence": 0.0, "note": "chardet not installed"}

    with open(path, "rb") as f:
        sample = f.read(65536)

    if not sample:
        return {"detected": "utf-8", "confidence": 1.0}

    # Strip BOM before chardet so it doesn't confuse the result
    has_bom = sample[:3] == b"\xef\xbb\xbf"
    if has_bom:
        sample = sample[3:]

    result = chardet.detect(sample)
    encoding = result.get("encoding") or "utf-8"
    confidence = result.get("confidence") or 0.0

    return {"detected": encoding, "confidence": round(confidence, 3), "bom": has_bom}


def detect_csv_format(path: str, encoding: str, bom: bool) -> dict:
    enc = "utf-8-sig" if bom else encoding
    try:
        with open(path, "r", encoding=enc, errors="replace", newline="") as f:
            sample = f.read(65536)
    except Exception as e:
        return {"error": str(e)}

    if not sample.strip():
        return {"delimiter": ",", "quotechar": '"', "has_header": False, "row_count_sample": 0}

    # Try csv.Sniffer
    delimiter = ","
    quotechar = '"'
    try:
        dialect = csv.Sniffer().sniff(sample[:4096], delimiters=",;\t|")
        delimiter = dialect.delimiter
        quotechar = dialect.quotechar or '"'
    except csv.Error:
        pass

    # Validate by checking column count consistency across first 50 rows
    lines = sample.split("\n")[:52]
    reader = csv.reader(io.StringIO("\n".join(lines)), delimiter=delimiter, quotechar=quotechar)
    counts = []
    rows = []
    for row in reader:
        counts.append(len(row))
        rows.append(row)

    if len(set(counts)) > 2:  # high variance → likely wrong delimiter
        # Fallback: try comma
        reader2 = csv.reader(io.StringIO("\n".join(lines)), delimiter=",")
        counts2 = [len(r) for r in reader2]
        if len(set(counts2)) <= len(set(counts)):
            delimiter = ","

    # Detect has_header
    has_header = True
    try:
        has_header = csv.Sniffer().has_header(sample[:4096])
    except csv.Error:
        pass

    # Count actual rows (approximate)
    row_count = sum(1 for _ in open(path, "r", encoding=enc, errors="replace"))
    if has_header and row_count > 0:
        row_count -= 1

    return {
        "delimiter": delimiter,
        "quotechar": quotechar,
        "has_header": has_header,
        "row_count": row_count,
    }


def is_likely_leading_zero_column(name: str, values: list) -> bool:
    non_null = [v for v in values if v and v.lower() not in NULL_REPRESENTATIONS]
    if not non_null:
        return False

    all_digits = all(v.isdigit() for v in non_null)
    has_leading_zero = any(v.startswith("0") and len(v) > 1 and v.isdigit() for v in non_null)

    # Values already show leading zeros
    if has_leading_zero:
        return True

    # Strong name signal + uniform-length digit strings (account numbers, etc.)
    if LEADING_ZERO_NAME_PATTERN.search(name) and all_digits:
        if len(set(len(v) for v in non_null)) == 1 and len(non_null[0]) > 3:
            return True

    return False


def classify_column(name: str, values: list) -> dict:
    non_null = [v for v in values if v and v.lower() not in NULL_REPRESENTATIONS]
    null_count = len(values) - len(non_null)
    null_representations_found = list({v for v in values if v.lower() in NULL_REPRESENTATIONS})

    risks = []
    inferred_type = "string"

    if not non_null:
        return {
            "inferred_type": "null",
            "null_count": null_count,
            "sample": values[:3],
            "risks": ["all_null"],
        }

    # Check for leading zeros before type inference
    if is_likely_leading_zero_column(name, non_null):
        risks.append("leading_zeros")
        inferred_type = "string"
        return {
            "inferred_type": inferred_type,
            "null_count": null_count,
            "null_representations": null_representations_found,
            "sample": non_null[:3],
            "risks": risks,
        }

    # Thousands separator / currency detection
    has_thousands = any(THOUSANDS_PATTERN.match(v) for v in non_null)
    has_european = any(EUROPEAN_NUMBER_PATTERN.match(v) for v in non_null)
    has_currency = any(CURRENCY_PATTERN.match(v) for v in non_null)
    if has_thousands or has_currency or has_european:
        if has_european:
            risks.append("european_number_format")
        else:
            risks.append("thousands_separator" if has_thousands else "currency_symbol")
        inferred_type = "float"
        return {
            "inferred_type": inferred_type,
            "null_count": null_count,
            "null_representations": null_representations_found,
            "sample": non_null[:3],
            "risks": risks,
        }

    # Try type inference
    type_counts = {"int": 0, "float": 0, "bool": 0, "date": 0, "string": 0}
    date_pattern = re.compile(
        r"^\d{1,4}[-/\.]\d{1,2}[-/\.]\d{1,4}$|^\d{1,2}/\d{1,2}/\d{2,4}$"
    )
    ambiguous_date_pattern = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$")
    has_ambiguous_date = False

    for v in non_null:
        if v.lower() in ("true", "false", "yes", "no"):
            type_counts["bool"] += 1
        elif date_pattern.match(v):
            type_counts["date"] += 1
            if ambiguous_date_pattern.match(v):
                has_ambiguous_date = True
        else:
            try:
                int(v)
                type_counts["int"] += 1
            except ValueError:
                try:
                    float(v)
                    type_counts["float"] += 1
                except ValueError:
                    type_counts["string"] += 1

    dominant = max(type_counts, key=lambda k: type_counts[k])
    total = len(non_null)
    dominant_pct = type_counts[dominant] / total if total > 0 else 0

    if dominant_pct < 0.8:
        inferred_type = "string"
        risks.append("mixed_types")
    else:
        inferred_type = dominant

    if has_ambiguous_date and inferred_type == "date":
        risks.append("ambiguous_date_format")

    if null_representations_found:
        risks.append("mixed_null_representations")

    return {
        "inferred_type": inferred_type,
        "null_count": null_count,
        "null_representations": null_representations_found if null_representations_found else None,
        "sample": non_null[:3],
        "risks": risks,
    }


def inspect_csv(path: str, encoding_info: dict, fmt_info: dict, sample_rows: int) -> list:
    enc = "utf-8-sig" if fmt_info.get("delimiter") and encoding_info.get("bom") else encoding_info["detected"]
    delimiter = fmt_info.get("delimiter", ",")
    quotechar = fmt_info.get("quotechar", '"')
    has_header = fmt_info.get("has_header", True)

    columns = []
    try:
        with open(path, "r", encoding=enc, errors="replace", newline="") as f:
            reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar)
            rows = []
            headers = None
            for i, row in enumerate(reader):
                if i == 0 and has_header:
                    headers = row
                    continue
                rows.append(row)
                if len(rows) >= sample_rows:
                    break

        if headers is None and rows:
            headers = [f"col_{i}" for i in range(len(rows[0]))]

        if not rows or not headers:
            return []

        for col_idx, col_name in enumerate(headers):
            values = [row[col_idx] if col_idx < len(row) else "" for row in rows]
            col_info = classify_column(col_name, values)
            columns.append({"name": col_name, **col_info})

    except Exception as e:
        columns.append({"error": str(e)})

    return columns


def detect_json_structure(path: str, encoding_info: dict) -> dict:
    enc = encoding_info.get("detected", "utf-8")
    try:
        with open(path, "r", encoding=enc, errors="replace") as f:
            first_line = f.readline().strip()
            second_line = f.readline().strip()

        # NDJSON: first line is a valid JSON object or array, second line is too
        try:
            obj1 = json.loads(first_line)
            if second_line:
                obj2 = json.loads(second_line)
                return {"structure": "ndjson", "sample_record": obj1}
        except json.JSONDecodeError:
            pass

        # Try full parse
        with open(path, "r", encoding=enc, errors="replace") as f:
            content = f.read()
        data = json.loads(content)

        if isinstance(data, list):
            if not data:
                return {"structure": "array_empty"}
            sample = data[0]
            # Check schema consistency
            keys = [set(r.keys()) if isinstance(r, dict) else set() for r in data]
            all_keys = set().union(*keys)
            min_keys = set.intersection(*keys) if keys else set()
            consistent = all_keys == min_keys
            has_nested = any(
                isinstance(v, (dict, list))
                for r in data if isinstance(r, dict)
                for v in r.values()
            )
            return {
                "structure": "array_of_objects",
                "record_count": len(data),
                "consistent_schema": consistent,
                "all_keys": sorted(all_keys),
                "has_nested": has_nested,
                "sample_record": data[0] if data else None,
            }
        elif isinstance(data, dict):
            # Check if it's object-of-objects
            all_vals_are_dicts = all(isinstance(v, dict) for v in data.values())
            return {
                "structure": "object_of_objects" if all_vals_are_dicts else "single_object",
                "key_count": len(data),
                "sample_keys": list(data.keys())[:5],
            }
    except Exception as e:
        return {"structure": "unknown", "error": str(e)}


def inspect_parquet(path: str) -> dict:
    try:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(path)
        schema = pf.schema_arrow
        meta = pf.metadata

        columns = []
        for i, field in enumerate(schema):
            col_info = {
                "name": field.name,
                "arrow_type": str(field.type),
                "risks": [],
            }
            t = field.type
            import pyarrow as pa
            if pa.types.is_dictionary(t):
                col_info["risks"].append("categorical")
            if pa.types.is_decimal(t):
                col_info["risks"].append("decimal_precision")
                col_info["precision"] = t.precision
                col_info["scale"] = t.scale
            if pa.types.is_struct(t):
                col_info["risks"].append("nested_struct")
                col_info["fields"] = [f.name for f in t]
            if pa.types.is_timestamp(t):
                col_info["unit"] = t.unit
                if t.unit == "ns":
                    col_info["risks"].append("nanosecond_precision_loss")
            columns.append(col_info)

        return {
            "row_count": meta.num_rows,
            "num_columns": meta.num_columns,
            "num_row_groups": meta.num_row_groups,
            "columns": columns,
        }
    except Exception as e:
        return {"error": str(e)}


def inspect_file(path: str, sample_rows: int = 100) -> dict:
    if not os.path.exists(path):
        return {"error": f"File not found: {path}", "file": path}

    size = os.path.getsize(path)
    if size == 0:
        return {"error": "File is empty", "file": path, "size_bytes": 0}

    fmt = detect_format(path)
    result = {
        "file": path,
        "size_bytes": size,
        "format": fmt,
        "compressed": any(path.endswith(ext) for ext in (".gz", ".zst", ".bz2", ".xz")),
    }

    if fmt == "parquet":
        result["parquet"] = inspect_parquet(path)
        all_risks = [r for col in result["parquet"].get("columns", []) for r in col.get("risks", [])]
        result["risks"] = list(set(all_risks))
        return result

    encoding_info = detect_encoding(path)
    result["encoding"] = encoding_info

    if fmt == "json":
        json_info = detect_json_structure(path, encoding_info)
        result["json"] = json_info
        result["risks"] = []
        if json_info.get("has_nested"):
            result["risks"].append("nested_json")
        if not json_info.get("consistent_schema", True):
            result["risks"].append("inconsistent_schema")
        return result

    # CSV path
    fmt_info = detect_csv_format(path, encoding_info["detected"], encoding_info.get("bom", False))
    result["delimiter"] = fmt_info.get("delimiter", ",")
    result["has_header"] = fmt_info.get("has_header", True)
    result["row_count"] = fmt_info.get("row_count", 0)

    columns = inspect_csv(path, encoding_info, fmt_info, sample_rows)
    result["columns"] = columns

    all_risks = []
    for col in columns:
        all_risks.extend(col.get("risks", []))
    # Add encoding risk if not UTF-8
    enc = encoding_info.get("detected", "utf-8") or "utf-8"
    if enc.lower().replace("-", "").replace("_", "") not in ("utf8", "ascii"):
        all_risks.append("non_utf8_encoding")
    if encoding_info.get("bom"):
        all_risks.append("bom_marker")

    # Null summary
    null_reprs = set()
    for col in columns:
        if col.get("null_representations"):
            null_reprs.update(col["null_representations"])
    if null_reprs:
        result["null_representations"] = sorted(null_reprs)

    result["risks"] = list(set(all_risks))
    return result


def main():
    parser = argparse.ArgumentParser(description="Inspect a data file and report its properties.")
    parser.add_argument("input", help="Path to the input file")
    parser.add_argument("--sample-rows", type=int, default=100, help="Rows to sample for type inference")
    args = parser.parse_args()

    report = inspect_file(args.input, args.sample_rows)

    if "error" in report:
        print(json.dumps(report, indent=2))
        sys.exit(1)

    print(json.dumps(report, indent=2, default=str))
    sys.exit(0)


if __name__ == "__main__":
    main()
