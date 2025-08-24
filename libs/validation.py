#In-DAG DQ
# libs/validation.py
# Validate
# - table not empty
# - required_not_null columns contain no NULLs
# - primary key uniqueness
# - expected_rows ± tolerance (from the JSON spec)
from __future__ import annotations
import os
from typing import Dict, Any, Tuple, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from libs.utils import dq_basic  # reuse your fast DQ

def _compare_schema(df: DataFrame, expected_schema: StructType, strict: bool = True) -> Tuple[bool, Dict[str, Any]]:
    exp_cols = [f.name for f in expected_schema.fields]
    act_cols = df.columns

    missing = [c for c in exp_cols if c not in act_cols]
    extra   = [c for c in act_cols if c not in exp_cols]
    types_diff = []
    for f in expected_schema.fields:
        if f.name in df.columns:
            act_type = str(df.schema[f.name].dataType)
            exp_type = str(f.dataType)
            if act_type != exp_type:
                types_diff.append({"column": f.name, "actual": act_type, "expected": exp_type})

    order_diff = (act_cols != exp_cols)

    if strict:
        ok = (not missing) and (not extra) and (not types_diff) and (not order_diff)
    else:
        # allow extra columns and order differences, but no missing columns or type mismatches
        ok = (not missing) and (not types_diff)

    return ok, {
        "missing": missing,
        "extra": extra,
        "types_diff": types_diff,
        "order_diff": order_diff,
        "actual": [(f.name, str(f.dataType)) for f in df.schema.fields],
        "expected": [(f.name, str(f.dataType)) for f in expected_schema.fields],
    }

def validate_partition(
    spark: SparkSession,
    partition_path: str,
    spec: Dict[str, Any],
    strict_schema: bool = True,
) -> Dict[str, Any]:
    """
    Re-read a written Parquet partition and validate:
      - not empty
      - required_not_null columns
      - primary key uniqueness
      - expected_rows ± tolerance (if provided)
      - schema equality vs spec['schema'] (strict by default)
    """
    if not os.path.isdir(partition_path):
        raise RuntimeError(f"Partition not found: {partition_path}")

    df = spark.read.parquet(partition_path)

    # fast/common checks via your existing helper
    dq_basic(df, spec.get("primary_key", []), spec.get("required_not_null", []), dq_cfg=spec.get("dq", {}))

    # strict schema check
    ok, diff = _compare_schema(df, spec["schema"], strict=strict_schema)
    if not ok:
        raise RuntimeError(f"Schema validation failed: {diff}")

    return {"rows": df.count(), "path": partition_path}
