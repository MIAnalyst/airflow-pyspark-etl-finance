#!/usr/bin/env python3
# jobs/ingest_store.py
#
# Bronze ingest for Store.csv with a preprocessing step that cleans raw files
#
# Driven by configs/schemas/Store.json 
# Requirements:
#   - libs/utils.py must be shipped via --py-files libs.zip so executors can import it.

from __future__ import annotations
import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from libs.utils import (
    load_spec_and_schema,
    make_bronze_ingest_parser,
    preprocess_file,
    read_csv_with_schema, # Spark CSV reader with explicit StructType + options
    dq_basic,             # basic DQ checks (row count, PK, required_not_null, etc.)
    write_bronze,         # write bronze layout with ingestion_date partition
)

def parse_args():
    ap = make_bronze_ingest_parser(
        "Bronze ingest for Store.csv (with preprocessing)",
        src_help="Path to source/Store.csv",
        schema_help="Path to configs/schemas/Store.json",
    )
    return ap.parse_args()

def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("bronze_ingest__Store")
        # .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        # 1) Load spec + resolve schema from DDL/JSON
        spec, schema = load_spec_and_schema(args.schema_json)

        fmt       = spec.get("format", {})
        dq_cfg    = spec.get("dq", {})
        write_cfg = spec.get("write", {})
        preproc   = spec.get("preprocess", {})

        pk  = spec.get("primary_key", [])
        req = spec.get("required_not_null", pk)

        # 2) Preprocess raw file to a UTF-8 temporary file if a preprocess block exists.
        src_path = args.src
        if preproc:
            src_path = preprocess_file(src_path, preproc)

        # 3) Read CSV with explicit StructType and provided options.
        df = read_csv_with_schema(
            spark=spark,
            path=src_path,
            schema=schema,
            fmt=fmt,
            preproc=None  # preprocessing already handled locally
        )

        # 4) DQ checks
        dq_basic(df=df, primary_key=pk, required_not_null=req, dq_cfg=dq_cfg)

        # 5) Write Bronze
        table_name   = spec.get("name") or spec.get("table") or "Store"
        partition_by = write_cfg.get("partition_by", [])  # usually [] for Bronze

        out = write_bronze(
            df=df,
            dst_root=args.dst,
            table_name=table_name,
            ingestion_date=args.ingestion_date,   #
            coalesce=args.coalesce,
            partition_by=partition_by
        )
        print(f"[OK] {table_name} -> {out}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
