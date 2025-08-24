#!/usr/bin/env python3
# jobs/transform_customer_silver.py
# Transform Customer Bronze -> Silver

import argparse, os, re
from pyspark.sql import SparkSession, functions as F
from libs.utils import resolve_ingestion_date
from libs.utils import (
    load_table_spec,
    dq_basic,
    write_silver,
    cast_columns_to_schema,
    fill_string_nulls,
    normalize_nulls,
)

def apply_customer_silver_rules(df):
    """
    Pure transform for unit tests:
      - Derive Type from PersonID/StoreID
      - Drop rowguid, ModifiedDate
      - No I/O
    """
    return (
        df.withColumn(
            "Type",
            F.when(F.col("PersonID").isNotNull() & F.col("StoreID").isNull(),  F.lit("Person"))
             .when(F.col("PersonID").isNull()     & F.col("StoreID").isNotNull(), F.lit("Store"))
             .when(F.col("PersonID").isNotNull()  & F.col("StoreID").isNotNull(),  F.lit("Both"))
             .otherwise(F.lit("UNK"))
        )
        .drop("rowguid", "ModifiedDate")
    )

def main():
    ap = argparse.ArgumentParser(description="Transform Customer Bronze -> Silver")
    ap.add_argument("--bronze-root", required=True, help="e.g., data/bronze/adventureworks")
    ap.add_argument("--silver-root", required=True, help="e.g., data/silver/adventureworks")
    ap.add_argument("--schema-json", required=True, help="configs/schemas/Customer_Silver.json")
    ap.add_argument("--ingestion-date", default="", help="YYYY-MM-DD or 'latest'")
    ap.add_argument("--coalesce", default=None, help="Coalesce to N files (optional)")
    args = ap.parse_args()

    spark = (
        SparkSession.builder
        .appName("silver_transform__Customer")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    spec = load_table_spec(args.schema_json)
    bronze_tbl = spec["name"].replace("_Silver", "")  # "Customer"
    ingestion_date = resolve_ingestion_date(args.bronze_root, bronze_tbl, args.ingestion_date)
    bronze_path = f"{args.bronze_root}/{bronze_tbl}/ingestion_date={ingestion_date}"

    print(f"[INFO] Reading bronze from: {bronze_path}")
    df = spark.read.parquet(bronze_path)

    # 1) Business rules (derive Type, drop unwanted)
    df = apply_customer_silver_rules(df)

    # 2) Normalize blanks -> NULLs, then cast & order to Silver schema
    df = normalize_nulls(df, None)
    df = cast_columns_to_schema(df, spec["schema"])

    # 3) Fill NULLs in string columns with "UNK" (excluding PK/fillna.exclude)
    fill_cfg = spec.get("fillna", {})
    exclude = set(fill_cfg.get("exclude", [])) | set(spec.get("primary_key", []))
    fill_value = fill_cfg.get("string", "UNK")
    df = fill_string_nulls(df, spec["schema"], exclude_cols=exclude, fill_value=fill_value)

    # 4) DQ checks
    dq_basic(df, spec["primary_key"], spec.get("required_not_null", []), dq_cfg=spec.get("dq"))

    # 5) Write Silver
    coalesce_n = int(args.coalesce) if args.coalesce not in (None, "", "None") else None
    out = write_silver(
        df=df,
        dst_root=args.silver_root,
        table_name=spec["name"],     # "Customer_Silver"
        ingestion_date=ingestion_date,
        coalesce=coalesce_n,
        partition_by=spec.get("write", {}).get("partition_by"),
    )

    print(f"[OK] Customer Bronze -> Silver at {out}")
    spark.stop()

if __name__ == "__main__":
    main()
