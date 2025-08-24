#!/usr/bin/env python3
# jobs/ingest_person.py

# Bronze ingest for Person.csv with a preprocessing step that cleans raw files
# BEFORE Spark reads them (e.g., convert UTF-16 -> UTF-8 and replace +| / &| tokens).

# Driven by configs/schemas/Person.json

from __future__ import annotations

from pyspark.sql import SparkSession

from libs.utils import (
    preprocess_file,
    make_bronze_ingest_parser,
    load_spec_and_schema,   # returns (spec_dict, schema_structtype) and sets spec["schema"]
    read_csv_with_schema,   # Spark CSV reader with explicit StructType + options
    dq_basic,               # basic DQ checks (row count, PK, required_not_null, etc.)
    write_bronze            # write bronze layout with ingestion_date partition
)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args():
    ap = make_bronze_ingest_parser(
        "Bronze ingest for Person.csv (with preprocessing)",
        src_help="Path to source/Person.csv",
        schema_help="Path to configs/schemas/Person.json",
    )
    return ap.parse_args()

def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("bronze_ingest__Person")
        # .config("spark.sql.session.timeZone", "UTC")   # optional
        .getOrCreate()
    )

    try:
        # 1) Load table spec (JSON) and resolve schema
        spec, schema = load_spec_and_schema(args.schema_json)

        fmt       = spec.get("format", {})
        dq_cfg    = spec.get("dq", {})
        write_cfg = spec.get("write", {})
        preproc   = spec.get("preprocess", {})

        pk  = spec.get("primary_key", [])
        req = spec.get("required_not_null", pk)

        # 2) Preprocess raw file (optional -> UTF-8 temp) BEFORE Spark reads it
        src_path = preprocess_file(args.src, preproc) if preproc else args.src

        # 3) Read CSV with explicit schema (no further preprocessing here)
        df = read_csv_with_schema(
            spark=spark,
            path=src_path,
            schema=schema,
            fmt=fmt,
            preproc=None
        )

        # 4) DQ checks
        dq_basic(df=df, primary_key=pk, required_not_null=req, dq_cfg=dq_cfg)

        # 5) Write Bronze parquet
        table_name   = spec.get("name") or spec.get("table") or "Person"
        partition_by = write_cfg.get("partition_by", [])

        # If you need strict column order, use the schema:
        # df = df.select([f.name for f in schema.fields])

        out_dir = write_bronze(
            df=df,
            dst_root=args.dst,
            table_name=table_name,
            ingestion_date=args.ingestion_date,  # None -> defaults to today inside write_bronze
            coalesce=args.coalesce,
            partition_by=partition_by
        )
        print(f"[OK] {table_name} -> {out_dir}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
