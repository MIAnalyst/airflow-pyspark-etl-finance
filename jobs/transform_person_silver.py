#!/usr/bin/env python3
# Transform Person Bronze -> Silver
#
# Steps:
# 1) Read Bronze partition for Person (auto-pick latest if --ingestion-date is "", missing, or "latest")
# 2) Expand XML column(s) listed in JSON (e.g., "Demographics") into flat columns
# 3) Drop columns listed in JSON (AdditionalContactInfo, rowguid, ModifiedDate, Demographics)
# 4) Cast columns to the Silver schema & order them (using libs.utils.cast_columns_to_schema)
# 5) Fill NULLs in STRING columns with "UNK" (excluding primary key / items in fillna.exclude)
# 6) DQ checks (not empty, required_not_null, PK uniqueness, expected_rows±tolerance)
# 7) Write Silver partition (parquet), optionally coalesced, partitioned as per JSON

import argparse
import os
import re
from libs.utils import resolve_ingestion_date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.column import Column
from libs.utils import (
    load_table_spec,
    dq_basic,
    write_silver,
    expand_xml_auto,
    cast_columns_to_schema,
    fill_string_nulls,
    normalize_nulls
)


def clean_iso_local_date_col(col: Column) -> Column:
    s = F.trim(col.cast("string"))
    s = F.regexp_replace(s, r"[T ]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:?\d{2})?$", "")
    s = F.regexp_replace(s, r"(?:[Zz]|[+-]\d{2}:?\d{2})$", "")
    s = F.substring(s, 1, 10)
    s = F.when(s == "", None).otherwise(s)
    return F.to_date(s, "yyyy-MM-dd")


def main():
    ap = argparse.ArgumentParser(description="Transform Person Bronze -> Silver")
    ap.add_argument("--bronze-root", required=True, help="Bronze root dir (e.g., data/bronze/adventureworks)")
    ap.add_argument("--silver-root", required=True, help="Silver root dir (e.g., data/silver/adventureworks)")
    ap.add_argument("--schema-json", required=True, help="Path to configs/schemas/Person_Silver.json")
    ap.add_argument("--ingestion-date", default="", help="Ingestion date (YYYY-MM-DD) or 'latest'")
    ap.add_argument("--coalesce", default=None, help="Coalesce to N files before write (optional)")
    args = ap.parse_args()

    spark = (
        SparkSession.builder
        .appName("silver_transform__Person")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    # --- Load table spec ---
    spec = load_table_spec(args.schema_json)
    schema_json = spec["schema"] 

    # --- Work out source (Bronze) and ingestion_date ---
    bronze_tbl = spec["name"].replace("_Silver", "")   # "Person"
    ingestion_date = resolve_ingestion_date(args.bronze_root, bronze_tbl, args.ingestion_date)
    bronze_path = f"{args.bronze_root}/{bronze_tbl}/ingestion_date={ingestion_date}"

    # --- Read Bronze parquet ---
    print(f"[INFO] Reading bronze from: {bronze_path}")
    df = spark.read.parquet(bronze_path)

    # --- Expand XML columns (e.g., Demographics) ---
    for col in spec.get("expand_xml", []):
        if col in df.columns:
            print(f"[INFO] Expanding XML column: {col}")
            df = expand_xml_auto(df, prefer_col=col)

    # --- Drop unwanted columns AFTER expansion ---
    drops = spec.get("drop", [])
    if drops:
        present = [c for c in drops if c in df.columns]
        missing = [c for c in drops if c not in df.columns]
        if present:
            print(f"[INFO] Dropping columns: {present}")
            df = df.drop(*present)
        if missing:
            print(f"[WARN] Columns to drop not found (ignored): {missing}")


    # --- Clean blanks -> NULLs before type casting ---
    df = normalize_nulls(df, None)

    # --- PRE-FIX BirthDate before generic casting ---
    # Strip a trailing 'Z'/'z' then parse as yyyy-MM-dd to avoid CAST errors.
    for c in ["BirthDate", "DateFirstPurchase"]:
        if c in df.columns:
            raw = f"__{c}_raw"
            df = (df.withColumn(raw, F.col(c).cast("string"))
                    .withColumn(c, clean_iso_local_date_col(F.col(raw)))
                    .drop(raw))

    # --- Cast to declared Silver schema & enforce column order ---
    df = cast_columns_to_schema(df, spec["schema"])

    # --- Fill NULLs in STRING columns with 'UNK' (exclude PK & fillna.exclude) ---
    fill_cfg = spec.get("fillna", {})
    exclude = set(fill_cfg.get("exclude", [])) | set(spec.get("primary_key", []))
    fill_value = fill_cfg.get("string", "UNK")
    df = fill_string_nulls(df, schema_json, exclude_cols=exclude, fill_value=fill_value)


    # derive string columns from the actual DF schema
    str_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in sorted(str_cols):
        stats = (df.select(
            F.sum(F.col(c).isNull().cast("int")).alias("nulls"),
            F.sum((F.trim(F.col(c)) == "").cast("int")).alias("empties"),
        ).first())
        print(f"[CHECK] {c}: nulls={stats['nulls']}, empties={stats['empties']}")

    # --- DQ check ---
    dq_basic(df, spec["primary_key"], spec["required_not_null"], dq_cfg=spec.get("dq"))

    coalesce_n = int(args.coalesce) if args.coalesce not in (None, "", "None") else None

    # --- Write Silver ---
    out = write_silver(
        df=df,
        dst_root=args.silver_root,
        table_name=spec["name"],           # "Person_Silver"
        ingestion_date=ingestion_date,
        coalesce=coalesce_n,
        partition_by=spec.get("write", {}).get("partition_by"),
    )

    print(f"[OK] Person Bronze -> Silver at {out}")
    spark.stop()


if __name__ == "__main__":
    main()
