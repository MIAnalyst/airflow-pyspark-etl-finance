#!/usr/bin/env python3
# Transform SalesTerritory Bronze -> Silver (drop rowguid, ModifiedDate)

import argparse, os, re
from pyspark.sql import SparkSession
from libs.utils import resolve_ingestion_date
from libs.utils import (
    load_table_spec,
    dq_basic,
    write_silver,
    cast_columns_to_schema,
    normalize_nulls,
    fill_string_nulls,
)


def main():
    ap = argparse.ArgumentParser(description="Transform SalesTerritory Bronze -> Silver")
    ap.add_argument("--bronze-root", required=True)
    ap.add_argument("--silver-root", required=True)
    ap.add_argument("--schema-json", required=True)  # configs/schemas/SalesTerritory_Silver.json
    ap.add_argument("--ingestion-date", default="", help="YYYY-MM-DD or 'latest'")
    ap.add_argument("--coalesce", default=None)
    args = ap.parse_args()

    spark = (
        SparkSession.builder
        .appName("silver_transform__SalesTerritory")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    spec = load_table_spec(args.schema_json)
    bronze_tbl = spec["name"].replace("_Silver", "")  # "SalesTerritory"
    ing = resolve_ingestion_date(args.bronze_root, bronze_tbl, args.ingestion_date)
    bronze_path = f"{args.bronze_root}/{bronze_tbl}/ingestion_date={ing}"
    print(f"[INFO] Reading bronze: {bronze_path}")
    df = spark.read.parquet(bronze_path)

    # drop only these two columns
    drops = ["rowguid", "ModifiedDate"]
    keep = [c for c in df.columns if c not in drops]
    df = df.select(*keep)

    # normalize blanks -> NULLs, then cast & order to target schema
    df = normalize_nulls(df, None)
    df = cast_columns_to_schema(df, spec["schema"])

    # optional string fill if configured in JSON
    fill_cfg = spec.get("fillna", {})
    if fill_cfg:
        exclude = set(fill_cfg.get("exclude", [])) | set(spec.get("primary_key", []))
        df = fill_string_nulls(df, spec["schema"], exclude_cols=exclude, fill_value=fill_cfg.get("string", "UNK"))

    # DQ
    dq_basic(df, spec["primary_key"], spec.get("required_not_null", []), dq_cfg=spec.get("dq"))

    coalesce_n = int(args.coalesce) if args.coalesce not in (None, "", "None") else None
    out = write_silver(
        df=df,
        dst_root=args.silver_root,
        table_name=spec["name"],        # "SalesTerritory_Silver"
        ingestion_date=ing,
        coalesce=coalesce_n,
        partition_by=spec.get("write", {}).get("partition_by"),
    )
    print(f"[OK] SalesTerritory -> {out}")
    spark.stop()

if __name__ == "__main__":
    main()
