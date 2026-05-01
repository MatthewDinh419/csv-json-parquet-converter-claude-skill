#!/usr/bin/env python3
"""
Phase 5: Validate a conversion output against its input and inspect report.
Checks row count, null inflation, and column type correctness for flagged columns.
Outputs a JSON report to stdout. Exit code 0 on pass, 1 on failure.

Usage:
    python validate.py <input> <output> --inspect <report.json>
                       [--null-tolerance 0.05]
"""

import argparse
import csv
import json
import os
import sys


def _count_rows(path: str, fmt: str, encoding: str = "utf-8", delimiter: str = ",",
                has_header: bool = True) -> int:
    if fmt == "parquet":
        import pyarrow.parquet as pq
        return pq.read_metadata(path).num_rows
    if fmt == "json":
        try:
            with open(path, "r", encoding=encoding) as f:
                first = f.readline().strip()
                second = f.readline().strip()
            try:
                json.loads(first)
                json.loads(second)
                with open(path, "r", encoding=encoding) as f:
                    return sum(1 for line in f if line.strip())
            except json.JSONDecodeError:
                pass
        except Exception:
            pass
        with open(path, "r", encoding=encoding) as f:
            data = json.load(f)
        return len(data) if isinstance(data, list) else 1
    # CSV
    with open(path, "r", encoding=encoding, errors="replace", newline="") as f:
        count = sum(1 for _ in csv.reader(f, delimiter=delimiter))
    return count - (1 if has_header else 0)


def _detect_out_format(path: str) -> str:
    ext = os.path.splitext(path.lower())[1].lstrip(".")
    if ext == "parquet":
        return "parquet"
    if ext in ("json", "ndjson", "jsonl"):
        return "json"
    return "csv"


def _get_output_column_types(output_path: str, out_fmt: str) -> dict:
    """Return {col_name: type_str} for the output file."""
    if out_fmt == "parquet":
        import pyarrow.parquet as pq
        schema = pq.read_schema(output_path)
        return {f.name: str(f.type) for f in schema}

    if out_fmt == "json":
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                first_line = f.readline().strip()
                second_line = f.readline().strip()
            try:
                rec1 = json.loads(first_line)
                json.loads(second_line)
                # NDJSON
            except json.JSONDecodeError:
                with open(output_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                rec1 = data[0] if isinstance(data, list) and data else {}
        except Exception:
            return {}
        return {k: type(v).__name__ for k, v in rec1.items()}

    # CSV — read first 10 rows and infer types
    types = {}
    try:
        with open(output_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = []
            for i, row in enumerate(reader):
                rows.append(row)
                if i >= 9:
                    break
        if not rows:
            return {}
        for col in rows[0]:
            values = [r[col] for r in rows if r.get(col)]
            if not values:
                types[col] = "null"
                continue
            all_int = all(_try_int(v) for v in values)
            all_float = all(_try_float(v) for v in values)
            types[col] = "int" if all_int else ("float" if all_float else "string")
    except Exception:
        pass
    return types


def _try_int(v: str) -> bool:
    try:
        int(v)
        return True
    except ValueError:
        return False


def _try_float(v: str) -> bool:
    try:
        float(v)
        return True
    except ValueError:
        return False


def _count_nulls_in_output(output_path: str, out_fmt: str, col_names: list) -> dict:
    """Return {col_name: null_count} for specified columns."""
    null_counts = {c: 0 for c in col_names}

    if out_fmt == "parquet":
        import pyarrow.parquet as pq
        table = pq.read_table(output_path, columns=[c for c in col_names if c])
        for col in col_names:
            if col in table.schema.names:
                null_counts[col] = table.column(col).null_count
        return null_counts

    if out_fmt == "json":
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                first = f.readline().strip()
                second = f.readline().strip()
            try:
                json.loads(first)
                json.loads(second)
                with open(output_path, "r", encoding="utf-8") as f:
                    rows = [json.loads(line) for line in f if line.strip()]
            except json.JSONDecodeError:
                with open(output_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                rows = data if isinstance(data, list) else [data]
        except Exception:
            return null_counts
        for row in rows:
            for col in col_names:
                if col not in row or row[col] is None:
                    null_counts[col] = null_counts.get(col, 0) + 1
        return null_counts

    # CSV
    try:
        with open(output_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                for col in col_names:
                    if col in row and (row[col] is None or row[col].strip() == ""):
                        null_counts[col] = null_counts.get(col, 0) + 1
    except Exception:
        pass
    return null_counts


def validate(input_path: str, output_path: str, report: dict,
             null_tolerance: float = 0.05) -> dict:
    issues = []
    warnings = []
    checks = {}

    in_fmt = report.get("format", "csv")
    out_fmt = _detect_out_format(output_path)

    encoding_info = report.get("encoding", {})
    encoding = encoding_info.get("detected", "utf-8") or "utf-8"
    bom = encoding_info.get("bom", False)
    if bom:
        encoding = "utf-8-sig"
    delimiter = report.get("delimiter", ",")
    has_header = report.get("has_header", True)

    # Row count check
    try:
        in_rows = report.get("row_count")
        if in_rows is None:
            in_rows = _count_rows(input_path, in_fmt, encoding, delimiter, has_header)
        out_rows = _count_rows(output_path, out_fmt)
        checks["row_count"] = {"input": in_rows, "output": out_rows, "match": in_rows == out_rows}
        if in_rows != out_rows:
            issues.append(f"Row count mismatch: input={in_rows}, output={out_rows}")
    except Exception as e:
        checks["row_count"] = {"error": str(e)}
        warnings.append(f"Could not verify row count: {e}")

    # Column type validation for flagged-risk columns
    type_checks = {}
    risk_cols = [c for c in report.get("columns", []) if c.get("risks")]
    if risk_cols and out_fmt != "parquet":  # parquet has strong typing; CSV/JSON need checking
        out_types = _get_output_column_types(output_path, out_fmt)
        for col in risk_cols:
            name = col["name"]
            risks = col.get("risks", [])
            if name not in out_types:
                continue
            actual_type = out_types[name]

            if "leading_zeros" in risks:
                # Must be string in output
                expected = "string" if out_fmt == "csv" else "str"
                ok = actual_type in ("string", "str")
                type_checks[name] = {"risk": "leading_zeros", "expected_type": "string",
                                     "actual_type": actual_type, "pass": ok}
                if not ok:
                    issues.append(f"Column '{name}' has leading_zeros risk but was written as {actual_type} (expected string)")
            elif "mixed_types" in risks:
                ok = actual_type in ("string", "str")
                type_checks[name] = {"risk": "mixed_types", "expected_type": "string",
                                     "actual_type": actual_type, "pass": ok}
                if not ok:
                    warnings.append(f"Column '{name}' has mixed_types risk; written as {actual_type}")

    checks["column_types"] = type_checks

    # Null inflation check for columns present in input
    null_inflation = {}
    col_names = [c["name"] for c in report.get("columns", []) if "name" in c]
    if col_names and in_fmt != "parquet":
        in_rows_for_null = checks.get("row_count", {}).get("input", 1) or 1
        out_null_counts = _count_nulls_in_output(output_path, out_fmt, col_names)
        for col in report.get("columns", []):
            name = col["name"]
            orig_null = col.get("null_count", 0) or 0
            out_null = out_null_counts.get(name, 0)
            increase = out_null - orig_null
            pct = increase / in_rows_for_null if in_rows_for_null > 0 else 0

            if pct > null_tolerance:
                issues.append(
                    f"Column '{name}': null count increased by {increase} rows ({pct:.1%}), "
                    f"exceeding tolerance {null_tolerance:.0%}. Likely parsing failure."
                )
                null_inflation[name] = {"original_nulls": orig_null, "output_nulls": out_null,
                                        "increase_pct": round(pct, 4), "status": "fail"}
            elif pct > 0.01:
                warnings.append(f"Column '{name}': null count increased by {increase} rows ({pct:.1%}). Investigate.")
                null_inflation[name] = {"original_nulls": orig_null, "output_nulls": out_null,
                                        "increase_pct": round(pct, 4), "status": "warn"}
            elif out_null != orig_null:
                null_inflation[name] = {"original_nulls": orig_null, "output_nulls": out_null,
                                        "increase_pct": round(pct, 4), "status": "ok"}

    checks["null_inflation"] = null_inflation

    passed = len(issues) == 0
    return {
        "passed": passed,
        "issues": issues,
        "warnings": warnings,
        "checks": checks,
    }


def main():
    parser = argparse.ArgumentParser(description="Validate a data conversion output.")
    parser.add_argument("input", help="Original input file")
    parser.add_argument("output", help="Converted output file")
    parser.add_argument("--inspect", required=True, help="Path to JSON report from inspect_data.py")
    parser.add_argument("--null-tolerance", type=float, default=0.05,
                        help="Max allowed null inflation fraction (default 0.05 = 5%%)")
    args = parser.parse_args()

    with open(args.inspect) as f:
        report = json.load(f)

    result = validate(args.input, args.output, report, null_tolerance=args.null_tolerance)

    print(json.dumps(result, indent=2, default=str))
    sys.exit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
