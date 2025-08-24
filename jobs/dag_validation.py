#!/usr/bin/env python3
"""
Generic DAG-side validator: re-read a single partition and validate it
using your JSON spec (schema + DQ rules).

"""
import argparse, os, json, sys
from copy import deepcopy

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from libs.utils import load_table_spec
from libs.validation import validate_partition


def _die(msg, details=None, code=1):
    print(json.dumps({"status": "FAIL", "msg": msg, "details": details or {}}, indent=2))
    sys.exit(code)


def _bool(s: str) -> bool:
    return str(s).strip().lower() in ("1", "true", "t", "yes", "y")


def main():
    ap = argparse.ArgumentParser(description="Validate a written table partition")
    ap.add_argument("--root", required=True, help="Layer root (e.g., data/bronze/adventureworks)")
    ap.add_argument("--table", required=True, help="Table dir (e.g., Person or Person_Silver)")
    ap.add_argument("--schema-json", required=True, help="Path to table schema JSON")
    ap.add_argument("--ingestion-date", required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--strict-schema",
        choices=["true", "false"],
        default="false",
        help="If true, enforce names+types+order; if false, ignore order (default).",
    )
    args = ap.parse_args()

    strict = _bool(args.strict_schema)
    part_path = os.path.join(args.root, args.table, f"ingestion_date={args.ingestion_date}")

    spark = (
        SparkSession.builder
        .appName(f"dag_validation__{args.table}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    print(f"[DEBUG] strict={strict}")
    print(f"[DEBUG] partition={part_path}")

    # Fast existence check
    if not os.path.isdir(part_path):
        _die(f"Partition path not found: {part_path}")

    # Load expected spec
    spec = load_table_spec(args.schema_json)

    # If not strict, proactively normalize the spec's column order to match the parquet,
    # so any downstream order check (even if present) won't fail the task.
    if not strict:
        try:
            df = spark.read.parquet(part_path)
            actual_names = df.schema.fieldNames()

            # Build a mapping from expected StructField by name
            expected_struct: StructType = spec["schema"]
            field_by_name = {f.name: f for f in expected_struct.fields}

            # Reorder schema: follow the parquet order but only include fields that are in the spec
            new_struct_fields = [field_by_name[name] for name in actual_names if name in field_by_name]

            # Preserve the original for reporting if something is missing
            expected_names = list(field_by_name.keys())
            missing_in_data = [n for n in expected_names if n not in actual_names]

            # If anything is missing, we still pass through to validator to fail properly.
            if new_struct_fields:
                spec = deepcopy(spec)  # avoid mutating the original
                spec["schema"] = StructType(new_struct_fields)

                # Also reorder the JSON 'fields' list (if present) to align with parquet order
                if "fields" in spec and isinstance(spec["fields"], list):
                    fdict = {f["name"]: f for f in spec["fields"]}
                    spec["fields"] = [fdict[name] for name in actual_names if name in fdict] + \
                                     [fdict[name] for name in expected_names if name not in actual_names and name in fdict]

                if missing_in_data:
                    print(f"[WARN] Missing columns present in spec but not in data: {missing_in_data}")
                print("[DEBUG] Spec column order normalized to match parquet (strict=false).")
            else:
                print("[WARN] Could not normalize spec order (no overlapping columns). Proceeding with original spec.")
        except Exception as e:
            # If anything goes wrong, continue with original spec; validator will surface details.
            print(f"[WARN] Failed to pre-read parquet for order normalization: {e}. Proceeding.")

    try:
        res = validate_partition(
            spark,
            partition_path=part_path,
            spec=spec,
            strict_schema=strict,
        )
        # Ensure consistent OK message format
        print(json.dumps({"status": "OK", "msg": "Validated", "details": res}, indent=2))
    except Exception as e:
        # Surface the error in a structured way
        _die(str(e))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
