#!/usr/bin/env python3
# Profile a Bronze table partition and write metrics to a single CSV.
# Output: <PROJECT_ROOT>/Profiler/<Table>_Bronze.csv

from __future__ import annotations
import argparse
import os
from typing import List, Tuple
from glob import glob
import shutil
import uuid

from pyspark.sql import SparkSession, functions as F, types as T

# IMPORTANT: classes, not instances
_NUMERIC_TYPES: Tuple[type, ...] = (
    T.ByteType,
    T.ShortType,
    T.IntegerType,
    T.LongType,
    T.FloatType,
    T.DoubleType,
    T.DecimalType,
)

def latest_partition(dir_: str) -> str:
    if not os.path.isdir(dir_):
        raise FileNotFoundError(f"Table directory not found: {dir_}")
    parts = [d for d in os.listdir(dir_) if d.startswith("ingestion_date=")]
    if not parts:
        raise FileNotFoundError(f"No ingestion_date= partitions under {dir_}")
    return sorted(p.split("=", 1)[1] for p in parts)[-1]

def parse_args():
    ap = argparse.ArgumentParser("Profile Bronze table -> CSV")
    ap.add_argument("--bronze-root", required=True, help="e.g., data/bronze/adventureworks")
    ap.add_argument("--table", required=True, help="e.g., Person | Store | Customer")
    ap.add_argument("--ingestion-date", default=None, help="YYYY-MM-DD (optional; latest if omitted)")
    ap.add_argument("--project-root", default=".", help="Project root for Profiler/ output (default: .)")
    ap.add_argument("--coalesce", type=int, default=1, help="Coalesce before collect (metrics are small).")
    return ap.parse_args()

def main():
    a = parse_args()
    table_clean = a.table.replace(".", "_")
    bronze_tbl_dir = os.path.join(a.bronze_root, table_clean)

    ingestion_date = a.ingestion_date.strip() if a.ingestion_date else latest_partition(bronze_tbl_dir)
    parquet_path = os.path.join(bronze_tbl_dir, f"ingestion_date={ingestion_date}")

    spark = SparkSession.builder.appName(f"profile_bronze__{table_clean}").getOrCreate()
    try:
        df = spark.read.parquet(parquet_path)
        row_count = df.count()

        rows: List[tuple] = []
        for field in df.schema.fields:
            col = field.name
            dtype = field.dataType.simpleString()

            nulls = df.select(F.sum(F.col(col).isNull().cast("long")).alias("n")).first()["n"] or 0
            distinct_count = df.select(F.countDistinct(F.col(col)).alias("d")).first()["d"]

            ws_only = None
            min_len = None
            max_len = None
            if isinstance(field.dataType, T.StringType):
                non_null = df.where(F.col(col).isNotNull())
                ws_only = non_null.select(
                    F.sum((F.length(F.trim(F.col(col))) == 0).cast("long")).alias("w")
                ).first()["w"] or 0
                lens = non_null.select(F.length(F.col(col)).alias("len"))
                if lens.head(1):
                    agg = lens.agg(F.min("len").alias("minl"), F.max("len").alias("maxl")).first()
                    min_len, max_len = agg["minl"], agg["maxl"]

            min_v = None
            max_v = None
            if isinstance(field.dataType, _NUMERIC_TYPES) or isinstance(field.dataType, (T.DateType, T.TimestampType)):
                agg2 = df.agg(F.min(F.col(col)).alias("minv"), F.max(F.col(col)).alias("maxv")).first()
                min_v = None if agg2["minv"] is None else str(agg2["minv"])
                max_v = None if agg2["maxv"] is None else str(agg2["maxv"])

            invalid = nulls + (ws_only or 0)

            rows.append((
                a.table, ingestion_date, col, dtype,
                row_count, nulls, ws_only, invalid,
                distinct_count, min_len, max_len, min_v, max_v
            ))

        metrics_schema = T.StructType([
            T.StructField("table", T.StringType(), False),
            T.StructField("ingestion_date", T.StringType(), False),
            T.StructField("column", T.StringType(), False),
            T.StructField("dtype", T.StringType(), False),
            T.StructField("row_count", T.LongType(), False),
            T.StructField("null_count", T.LongType(), True),
            T.StructField("whitespace_only_count", T.LongType(), True),
            T.StructField("invalid_count", T.LongType(), True),  # nulls + blanks
            T.StructField("distinct_count", T.LongType(), True),
            T.StructField("min_length", T.IntegerType(), True),
            T.StructField("max_length", T.IntegerType(), True),
            T.StructField("min_value", T.StringType(), True),
            T.StructField("max_value", T.StringType(), True),
        ])
        metrics_sdf = spark.createDataFrame(rows, schema=metrics_schema).coalesce(a.coalesce)

        out_dir = os.path.join(a.project_root, "Profiler")
        os.makedirs(out_dir, exist_ok=True)
        out_csv_path = os.path.join(out_dir, f"{table_clean}_Bronze.csv")

        # Spark writes CSVs to a directory; use a temp dir then rename the single part file.
        tmp_dir = os.path.join(out_dir, f".tmp_{table_clean}_{uuid.uuid4().hex}")
        (
            metrics_sdf.orderBy("column")
            .coalesce(1)                     # ensure one output file
            .write.mode("overwrite")
            .option("header", True)
            .csv(tmp_dir)
        )

        # find the part file Spark created and move/rename it
        part_files = glob(os.path.join(tmp_dir, "part-*.csv")) or glob(os.path.join(tmp_dir, "part-*"))
        if not part_files:
            raise FileNotFoundError(f"No CSV part file found in {tmp_dir}")
        shutil.move(part_files[0], out_csv_path)

        # remove Spark’s temp directory (_SUCCESS, etc.)
        shutil.rmtree(tmp_dir, ignore_errors=True)

        print(f"[OK] Profiled {a.table} {ingestion_date}")
        print(f"[OUT] {out_csv_path}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
