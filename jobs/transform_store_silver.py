#!/usr/bin/env python3
# Transform Store Bronze -> Silver

import argparse
from pyspark.sql import SparkSession
from libs.utils import (
    load_table_spec,
    dq_basic,
    write_silver,
    expand_xml_auto,
    cast_columns_to_schema,   # <-- new import
)

def main():
    ap = argparse.ArgumentParser(description="Transform Store Bronze -> Silver")
    ap.add_argument("--bronze-root", required=True, help="Bronze root dir (e.g., data/bronze/adventureworks)")
    ap.add_argument("--silver-root", required=True, help="Silver root dir (e.g., data/silver/adventureworks)")
    ap.add_argument("--schema-json", required=True, help="Path to configs/schemas/Store_Silver.json")
    ap.add_argument("--ingestion-date", required=True, help="Ingestion date (YYYY-MM-DD)")
    ap.add_argument("--coalesce", default=None, help="Coalesce to N files before write (optional)")
    args = ap.parse_args()

    spark = (
        SparkSession.builder
        .appName("silver_transform__Store")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    # --- Load table spec ---
    spec = load_table_spec(args.schema_json)

    # --- Read Bronze parquet ---
    # JSON name is Store_Silver => Bronze folder is "Store"
    bronze_tbl = spec["name"].replace("_Silver", "")
    bronze_path = f"{args.bronze_root}/{bronze_tbl}/ingestion_date={args.ingestion_date}"
    df = spark.read.parquet(bronze_path)

    # --- Expand XML Demographics into flat columns (as STRINGs) ---
    for col in spec.get("expand_xml", []):
        if col in df.columns:
            df = expand_xml_auto(df, prefer_col=col)

    # --- Drop unwanted columns AFTER expansion ---
    drops = spec.get("drop", [])
    if drops:
        df = df.drop(*drops)

    # --- Cast columns to declared Silver schema & order them ---
    df = cast_columns_to_schema(df, spec["schema"])

    # --- DQ check (not-empty, PK not null, PK unique, row count expectation, etc.) ---
    dq_basic(df, spec["primary_key"], spec["required_not_null"], dq_cfg=spec.get("dq"))

    # --- Write Silver ---
    out = write_silver(
        df=df,
        dst_root=args.silver_root,
        table_name=spec["name"],
        ingestion_date=args.ingestion_date,
        coalesce=args.coalesce,
        partition_by=spec.get("write", {}).get("partition_by"),
    )

    print(f"[OK] Store Bronze -> Silver at {out}")
    spark.stop()

if __name__ == "__main__":
    main()
