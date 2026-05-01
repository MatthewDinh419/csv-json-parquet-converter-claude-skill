#!/usr/bin/env python3
"""
Benchmark harness: compare baseline (naive pandas) vs skill-guided conversions.

Usage:
    python tests/test_conversions.py --baseline   # Run naive conversions
    python tests/test_conversions.py --skill       # Run skill-guided conversions
    python tests/test_conversions.py --both        # Run both and compare
    python tests/test_conversions.py --fixture 05  # Run single fixture
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
import traceback

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
SCRIPTS = os.path.join(os.path.dirname(__file__), "..", "scripts")
SKILL_SCRIPTS = os.path.expanduser("~/.claude/skills/data-format-converter/scripts")

# Each test case: (fixture_file, output_extension, validation_fn, description)
# validation_fn(output_path) -> (passed: bool, reason: str)

def _read_json_output(path):
    with open(path, "r", encoding="utf-8") as f:
        first = f.readline().strip()
        second = f.readline().strip()
    try:
        r1 = json.loads(first)
        json.loads(second)
        with open(path) as f:
            return [json.loads(l) for l in f if l.strip()]
    except json.JSONDecodeError:
        pass
    with open(path) as f:
        return json.load(f)


def _read_csv_output(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def validate_leading_zero_zips_json(path):
    try:
        rows = _read_json_output(path)
        zips = [r.get("zip_code") for r in rows]
        if all(isinstance(z, str) and z.startswith("0") for z in zips):
            return True, "zip_code preserved as string with leading zeros"
        return False, f"zip_code values lost leading zeros: {zips}"
    except Exception as e:
        return False, str(e)


def validate_accented_encoding_json(path):
    try:
        rows = _read_json_output(path)
        names = [r.get("name") for r in rows]
        expected = ["René", "Ångström", "Müller"]
        if names == expected:
            return True, "Accented characters preserved correctly"
        return False, f"Names incorrect: {names}"
    except Exception as e:
        return False, str(e)


def validate_semicolon_parquet(path):
    try:
        import pyarrow.parquet as pq
        import pandas as pd
        df = pq.read_table(path).to_pandas()
        if len(df) != 3:
            return False, f"Expected 3 rows, got {len(df)}"
        if list(df.columns) != ["id", "name", "amount"]:
            return False, f"Unexpected columns: {list(df.columns)}"
        amounts = df["amount"].tolist()
        expected = [1234.56, 7890.12, 3456.78]
        if amounts == expected:
            return True, f"European numbers parsed correctly: {amounts}"
        return False, f"Amount values incorrect: {amounts}, expected {expected}"
    except Exception as e:
        return False, str(e)


def validate_mixed_nulls_json(path):
    try:
        rows = _read_json_output(path)
        if len(rows) != 5:
            return False, f"Expected 5 rows, got {len(rows)}"
        # score should be null for rows 2 and 3
        null_scores = [r for r in rows if r.get("score") is None]
        if len(null_scores) >= 2:
            return True, f"{len(null_scores)} null scores correctly detected"
        return False, f"Expected nulls in score column, got: {[r.get('score') for r in rows]}"
    except Exception as e:
        return False, str(e)


def validate_bom_json(path):
    try:
        rows = _read_json_output(path)
        if not rows:
            return False, "No rows"
        keys = list(rows[0].keys())
        if "id" in keys and not any("﻿" in k for k in keys):
            return True, "BOM stripped, column name is clean 'id'"
        return False, f"BOM present in column name or 'id' missing: {keys}"
    except Exception as e:
        return False, str(e)


def validate_nested_json_csv(path):
    try:
        rows = _read_csv_output(path)
        if not rows:
            return False, "No rows"
        cols = list(rows[0].keys())
        if "address.city" in cols:
            return True, "Nested JSON flattened with dot notation"
        return False, f"Expected 'address.city' column, got: {cols}"
    except Exception as e:
        return False, str(e)


def validate_categorical_parquet_csv(path):
    try:
        rows = _read_csv_output(path)
        if len(rows) != 5:
            return False, f"Expected 5 rows, got {len(rows)}"
        depts = [r.get("department") for r in rows]
        if all(isinstance(d, str) and d in ("Eng", "Sales", "HR") for d in depts):
            return True, "Categorical values decoded correctly"
        return False, f"Categorical values wrong: {depts}"
    except Exception as e:
        return False, str(e)


def validate_decimal_parquet_csv(path):
    try:
        rows = _read_csv_output(path)
        prices = [r.get("price") for r in rows]
        # Should be strings preserving decimal precision
        if prices == ["1234.56", "89.99", "2345.00"]:
            return True, "Decimal values preserved as strings"
        return False, f"Decimal values: {prices}"
    except Exception as e:
        return False, str(e)


def validate_nested_struct_parquet_csv(path):
    try:
        rows = _read_csv_output(path)
        cols = list(rows[0].keys()) if rows else []
        if "address.city" in cols and "address.zip" in cols:
            return True, "Struct columns flattened with dot notation"
        return False, f"Expected flattened struct columns, got: {cols}"
    except Exception as e:
        return False, str(e)


def validate_phone_account_json(path):
    try:
        rows = _read_json_output(path)
        accounts = [r.get("account_id") for r in rows]
        phones = [r.get("phone") for r in rows]
        acct_ok = all(isinstance(a, str) and a.startswith("0") for a in accounts)
        phone_ok = all(isinstance(p, str) and p.startswith("0") for p in phones)
        if acct_ok and phone_ok:
            return True, "account_id and phone preserved as strings with leading zeros"
        return False, f"account_id={accounts}, phone={phones}"
    except Exception as e:
        return False, str(e)


def validate_ndjson_parquet(path):
    try:
        import pyarrow.parquet as pq
        t = pq.read_table(path)
        if t.num_rows == 3 and "id" in t.schema.names:
            return True, f"NDJSON → Parquet: {t.num_rows} rows, {t.num_columns} columns"
        return False, f"rows={t.num_rows}, schema={t.schema.names}"
    except Exception as e:
        return False, str(e)


TESTS = [
    ("05_leading_zero_zips.csv", "json", validate_leading_zero_zips_json,
     "Leading-zero zip codes preserved as strings"),
    ("02_windows1252_accented.csv", "json", validate_accented_encoding_json,
     "Windows-1252 accented characters decoded correctly"),
    ("03_semicolon_delimited.csv", "parquet", validate_semicolon_parquet,
     "Semicolon delimiter + European numbers parsed correctly"),
    ("06_mixed_nulls.csv", "json", validate_mixed_nulls_json,
     "Mixed null representations detected and normalized"),
    ("09_utf8_bom.csv", "json", validate_bom_json,
     "UTF-8 BOM stripped from file and column names"),
    ("19_nested_json.json", "csv", validate_nested_json_csv,
     "Nested JSON flattened with dot notation to CSV"),
    ("24_categorical_columns.parquet", "csv", validate_categorical_parquet_csv,
     "Parquet categorical columns decoded to string values"),
    ("25_decimal_types.parquet", "csv", validate_decimal_parquet_csv,
     "Parquet decimal types preserved as strings (no float conversion)"),
    ("26_nested_struct.parquet", "csv", validate_nested_struct_parquet_csv,
     "Parquet struct columns flattened with dot notation"),
    ("17_phone_account_ids.csv", "json", validate_phone_account_json,
     "Phone numbers and account IDs preserved as strings"),
    ("18_ndjson.json", "parquet", validate_ndjson_parquet,
     "NDJSON → Parquet using pyarrow.json.read_json"),
]


def run_baseline(fixture_path, out_path, out_ext):
    """Naive pandas one-liner — no inspection, no explicit params."""
    try:
        import pandas as pd
        in_ext = os.path.splitext(fixture_path)[1].lstrip(".")

        if in_ext == "csv" and out_ext == "json":
            pd.read_csv(fixture_path).to_json(out_path, orient="records", indent=2)
        elif in_ext == "csv" and out_ext == "parquet":
            pd.read_csv(fixture_path).to_parquet(out_path, engine="pyarrow")
        elif in_ext == "json" and out_ext == "csv":
            pd.read_json(fixture_path).to_csv(out_path, index=False)
        elif in_ext == "json" and out_ext == "parquet":
            pd.read_json(fixture_path).to_parquet(out_path, engine="pyarrow")
        elif in_ext == "parquet" and out_ext == "csv":
            pd.read_parquet(fixture_path, engine="pyarrow").to_csv(out_path, index=False)
        elif in_ext == "parquet" and out_ext == "json":
            pd.read_parquet(fixture_path, engine="pyarrow").to_json(out_path, orient="records", indent=2)
        else:
            return False, f"Unsupported baseline: {in_ext} → {out_ext}"
        return True, "conversion ran without exception"
    except Exception as e:
        return False, f"Exception: {e}"


def run_skill(fixture_path, out_path):
    """Skill-guided conversion via inspect → convert → validate pipeline."""
    scripts_dir = SCRIPTS if os.path.exists(os.path.join(SCRIPTS, "inspect_data.py")) else SKILL_SCRIPTS
    inspect_script = os.path.join(scripts_dir, "inspect_data.py")
    convert_script = os.path.join(scripts_dir, "convert.py")
    validate_script = os.path.join(scripts_dir, "validate.py")

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
        report_path = tf.name

    try:
        # Inspect
        r = subprocess.run(
            [sys.executable, inspect_script, fixture_path],
            capture_output=True, text=True
        )
        if r.returncode != 0:
            return False, f"inspect failed: {r.stderr or r.stdout}"
        with open(report_path, "w") as f:
            f.write(r.stdout)

        # Convert
        r = subprocess.run(
            [sys.executable, convert_script, fixture_path, out_path, "--inspect", report_path],
            capture_output=True, text=True
        )
        if r.returncode != 0:
            return False, f"convert failed: {r.stderr or r.stdout}"

        # Validate
        r = subprocess.run(
            [sys.executable, validate_script, fixture_path, out_path, "--inspect", report_path],
            capture_output=True, text=True
        )
        if r.returncode != 0:
            val = json.loads(r.stdout) if r.stdout.strip() else {}
            return False, f"validate failed: {val.get('issues', r.stderr)}"

        return True, "pipeline passed"
    except Exception as e:
        return False, str(e)
    finally:
        if os.path.exists(report_path):
            os.unlink(report_path)


def run_tests(mode: str, fixture_filter: str = None):
    results = []

    for fixture_file, out_ext, validation_fn, description in TESTS:
        if fixture_filter and fixture_filter not in fixture_file:
            continue

        fixture_path = os.path.join(FIXTURES, fixture_file)
        if not os.path.exists(fixture_path):
            print(f"  SKIP  {fixture_file} (fixture not found)")
            continue

        with tempfile.NamedTemporaryFile(suffix=f".{out_ext}", delete=False) as tf:
            out_path = tf.name

        try:
            if mode == "baseline":
                ran, run_reason = run_baseline(fixture_path, out_path, out_ext)
            else:
                ran, run_reason = run_skill(fixture_path, out_path)

            if not ran:
                results.append((fixture_file, description, False, f"Conversion failed: {run_reason}"))
                print(f"  FAIL  {fixture_file}: {run_reason}")
                continue

            passed, reason = validation_fn(out_path)
            results.append((fixture_file, description, passed, reason))
            status = "PASS" if passed else "FAIL"
            print(f"  {status}  {fixture_file}: {reason}")

        except Exception as e:
            results.append((fixture_file, description, False, str(e)))
            print(f"  ERR   {fixture_file}: {traceback.format_exc()}")
        finally:
            if os.path.exists(out_path):
                os.unlink(out_path)

    return results


def main():
    parser = argparse.ArgumentParser(description="Benchmark baseline vs skill-guided conversions.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--baseline", action="store_true")
    group.add_argument("--skill", action="store_true")
    group.add_argument("--both", action="store_true")
    parser.add_argument("--fixture", help="Filter by fixture filename substring")
    args = parser.parse_args()

    if args.both:
        print("\n=== BASELINE (naive pandas) ===")
        baseline = run_tests("baseline", args.fixture)
        baseline_pass = sum(1 for _, _, p, _ in baseline if p)

        print(f"\n=== SKILL-GUIDED ===")
        skill = run_tests("skill", args.fixture)
        skill_pass = sum(1 for _, _, p, _ in skill if p)

        total = len(baseline)
        print(f"\n{'='*50}")
        print(f"Baseline:     {baseline_pass}/{total} passed ({100*baseline_pass/total:.0f}%)")
        print(f"Skill-guided: {skill_pass}/{total} passed ({100*skill_pass/total:.0f}%)")
        print(f"Lift:         +{skill_pass - baseline_pass} tests")
        target_skill = 0.90
        target_baseline = 0.60
        skill_ok = skill_pass / total >= target_skill
        baseline_ok = baseline_pass / total <= target_baseline
        print(f"\nTarget: skill ≥{target_skill:.0%}, baseline ≤{target_baseline:.0%}")
        print(f"  Skill target {'MET' if skill_ok else 'NOT MET'}")
        print(f"  Baseline target {'MET' if baseline_ok else 'NOT MET'}")
    elif args.baseline:
        print("\n=== BASELINE (naive pandas) ===")
        results = run_tests("baseline", args.fixture)
        passed = sum(1 for _, _, p, _ in results if p)
        print(f"\n{passed}/{len(results)} passed")
    else:
        print("\n=== SKILL-GUIDED ===")
        results = run_tests("skill", args.fixture)
        passed = sum(1 for _, _, p, _ in results if p)
        print(f"\n{passed}/{len(results)} passed")


if __name__ == "__main__":
    main()
