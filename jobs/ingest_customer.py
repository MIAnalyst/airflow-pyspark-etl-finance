#!/usr/bin/env python3
# jobs/ingest_customer.py
# Purpose: Ingest Customer.csv -> Bronze Parquet with DQ & optional partitioning.

from __future__ import annotations
import argparse
import json
import pathlib

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from libs.utils import (
    load_spec_and_schema,
    preprocess_file,
    make_bronze_ingest_parser,
    read_csv_with_schema,  # Spark CSV reader with explicit StructType + options
    dq_basic,              # row count / PK / required_not_null
    write_bronze,          # write bronze with ingestion_date partition
)

def parse_args():
    ap = make_bronze_ingest_parser(
        "Bronze ingest for Customer.csv (with preprocessing)",
        src_help="Path to source/Customer.csv",
        schema_help="Path to configs/schemas/Customer.json",
    )
    return ap.parse_args()

def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("bronze_ingest__Customer")
        # .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    try:
        # 1) Spec + schema
        spec, schema = load_spec_and_schema(args.schema_json)

        fmt       = spec.get("format", {})
        dq_cfg    = spec.get("dq", {})
        write_cfg = spec.get("write", {})
        preproc   = spec.get("preprocess", {})

        pk  = spec.get("primary_key", [])
        req = spec.get("required_not_null", pk)

        # 2) Preprocess raw file if configured
        src_path = args.src
        if preproc:
            src_path = preprocess_file(src_path, preproc)

        # 3) Read CSV with explicit schema
        df = read_csv_with_schema(
            spark=spark,
            path=src_path,
            schema=schema,
            fmt=fmt,
            preproc=None  # already handled
        )

        # 4) DQ
        dq_basic(df=df, primary_key=pk, required_not_null=req, dq_cfg=dq_cfg)

        # 5) Write Bronze
        out_path = write_bronze(
            df=df,
            dst_root=args.dst,
            table_name=spec.get("name", "Customer"),
            ingestion_date=args.ingestion_date,
            coalesce=args.coalesce,
            partition_by=write_cfg.get("partition_by", []),
        )
        print(f"[OK] Customer -> {out_path}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
