#!/usr/bin/env python3
# Build DimCustomer (Gold) from Silver: Customer, SalesTerritory, Person, Store
from __future__ import annotations

import argparse, os, re
from typing import Dict, List, Optional
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.column import Column

from libs.utils import (
    load_table_spec,
    cast_columns_to_schema,
    dq_basic,
    write_silver as write_gold,   # reuse writer (root points to gold/)
)

# ---------------------------
# Snapshot picking utilities
# ---------------------------
def list_partitions(base: str) -> List[str]:
    return sorted([re.sub(r"^ingestion_date=", "", d)
                   for d in os.listdir(base) if d.startswith("ingestion_date=")])

def latest_for_table(base_dir: str) -> str:
    parts = list_partitions(base_dir)
    if not parts:
        raise RuntimeError(f"No partitions under {base_dir}")
    return parts[-1]

def resolve_dates_per_table(silver_root: str, tables: List[str], requested: Optional[str]) -> Dict[str, str]:
    """
    If requested is provided and not 'latest', use it for all tables.
    Otherwise pick each table's latest partition independently.
    """
    dates: Dict[str, str] = {}
    for t in tables:
        base = os.path.join(silver_root, t)
        if not os.path.isdir(base):
            raise RuntimeError(f"Missing silver table dir: {base}")
        if requested and requested.lower() != "latest":
            dates[t] = requested
        else:
            dates[t] = latest_for_table(base)
    return dates

def resolve_date_common(silver_root: str, tables: List[str], requested: Optional[str]) -> str:
    """Pick a single date present in ALL tables; fail loudly if none."""
    if requested and requested.lower() != "latest":
        return requested
    per_table = {}
    for t in tables:
        base = os.path.join(silver_root, t)
        if not os.path.isdir(base):
            raise RuntimeError(f"Missing silver table dir: {base}")
        dates = list_partitions(base)
        if not dates:
            raise RuntimeError(f"No partitions under {base}")
        per_table[t] = set(dates)
    inter = set.intersection(*per_table.values())
    if not inter:
        details = "\n".join([f"- {t}: {', '.join(sorted(v))}" for t, v in per_table.items()])
        raise RuntimeError("No common ingestion_date across Silver tables.\n" + details)
    return max(inter)

# ---------------------------
# Pure transformation helpers
# ---------------------------
def derive_origin_year_col(type_col: Column, birthdate_col: Column, yearopened_col: Column) -> Column:
    """Type=Person -> year(BirthDate); Type in {Store,Both} -> YearOpened; else NULL."""
    return (
        F.when(type_col == "Person", F.substring(birthdate_col.cast("string"), 1, 4).cast("int"))
         .when(type_col.isin("Store", "Both"), yearopened_col.cast("int"))
         .otherwise(F.lit(None).cast("int"))
    )

def map_business_col(type_col: Column, occupation_col: Column, business_type_col: Column) -> Column:
    """Pick Occupation for Person else BusinessType, then map short codes to labels."""
    base = F.when(type_col == "Person", occupation_col).otherwise(business_type_col)
    b = F.trim(base)
    return (
        F.when(b == "BM", F.lit("Bike Manufacturer"))
         .when(b == "BS", F.lit("Bike Shop"))
         .when(b == "OS", F.lit("Outdoor Supplier"))
         .otherwise(base)
    )

def band_yearly_income_col(type_col: Column, annual_rev_col: Column, existing_income_col: Column) -> Column:
    """For Store/Both, bucket AnnualRevenue into bands; otherwise keep existing YearlyIncome."""
    bands = (
        F.when(annual_rev_col <= 25000, F.lit("0-25000"))
         .when((annual_rev_col >= 25001) & (annual_rev_col <= 50000),  F.lit("25001-50000"))
         .when((annual_rev_col >= 50001) & (annual_rev_col <= 75000),  F.lit("50001-75000"))
         .when((annual_rev_col >= 75001) & (annual_rev_col <= 100000), F.lit("75001-100000"))
         .when(annual_rev_col > 100000,                                 F.lit("100000+"))  # map directly
         .otherwise(F.lit("0"))
    )
    return F.when(type_col.isin("Store", "Both"), bands).otherwise(existing_income_col)

def add_scd_v1(df, effective_from="2014-09-12"):
    """Add SCDv1 columns."""
    return (
        df.withColumn("effective_from", F.to_date(F.lit(effective_from)))
          .withColumn("effective_to",   F.lit(None).cast("date"))
          .withColumn("is_current",     F.lit(1))
    )

def build_dim_customer_frame(cust_df, person_df, store_df, terr_df, target_schema):
    """
    Join Customer + SalesTerritory + Person + Store, apply business derivations,
    drop staging cols, and cast/order to target schema.
    """
    # Territory (left join) + alias
    d = (cust_df
         .join(terr_df.select("TerritoryID", "CountryRegionCode"), on="TerritoryID", how="left")
         .withColumnRenamed("CountryRegionCode", "CustCountryCode")
         .select(
             "CustomerID",
             "Type",
             "CustCountryCode",
             "PersonID",
             "StoreID"
         ))

    # Select only the fields we need from Store & Person and rename join keys to avoid collisions
    store_sel = store_df.select(
        F.col("BusinessEntityID").alias("Store_BusinessEntityID"),
        "YearOpened", "NumberEmployees", "BusinessType", "AnnualRevenue"
    )
    person_sel = person_df.select(
        F.col("BusinessEntityID").alias("Person_BusinessEntityID"),
        "BirthDate", "Occupation", "YearlyIncome"
    )

    # Join to Store & Person (left)
    d = (d
         .join(store_sel,  d.StoreID  == F.col("Store_BusinessEntityID"),  how="left")
         .join(person_sel, d.PersonID == F.col("Person_BusinessEntityID"), how="left"))

    # Derivations
    d = d.withColumn("origin_year",     derive_origin_year_col(F.col("Type"), F.col("BirthDate"), F.col("YearOpened")))
    d = d.withColumn("NumberEmployees", F.coalesce(F.col("NumberEmployees"), F.lit(0)).cast("int"))
    d = d.withColumn("Business",        map_business_col(F.col("Type"), F.col("Occupation"), F.col("BusinessType")))
    d = d.withColumn("YearlyIncome",    band_yearly_income_col(F.col("Type"), F.col("AnnualRevenue"), F.col("YearlyIncome")))
    d = add_scd_v1(d)

    # Drop columns no longer needed
    d = d.drop(
        "BusinessEntityID", "PersonID", "StoreID", "Specialty",
        "BankName", "Brands", "Internet", "CommuteDistance", "AnnualSales",
        "DateFirstPurchase", "Education", "Gender", "SquareFeet",
        "HomeOwnerFlag", "MaritalStatus", "NumberCarsOwned",
        "NumberChildrenAtHome", "TotalChildren", "TotalPurchaseYTD",
        "YearOpened", "BirthDate", "BusinessType", "Occupation",
        "AnnualRevenue", "PersonType",
        "Store_BusinessEntityID", "Person_BusinessEntityID"
    )

    # Cast & order to final schema
    return cast_columns_to_schema(d, target_schema)

# ---------------------------
# main: I/O + wiring
# ---------------------------
def main():
    ap = argparse.ArgumentParser(description="Build DimCustomer (Gold) from Silver tables")
    ap.add_argument("--silver-root", required=True, help="e.g., data/silver/adventureworks")
    ap.add_argument("--gold-root",   required=True, help="e.g., data/gold/adventureworks")
    ap.add_argument("--schema-json", required=True, help="configs/schemas/DimCustomer.json")
    ap.add_argument("--ingestion-date", default="latest", help="YYYY-MM-DD or 'latest'")
    ap.add_argument("--snapshot-mode", choices=["per-table", "common"], default="per-table",
                    help="Choose latest per table (default) or a common date across all tables.")
    ap.add_argument("--coalesce", default=None, help="Coalesce to N files (optional)")
    args = ap.parse_args()

    spark = (
        SparkSession.builder
        .appName("gold_build__DimCustomer")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    spec = load_table_spec(args.schema_json)
    # Silver table folder names
    T_CUSTOMER = "Customer_Silver"
    T_PERSON   = "Person_Silver"
    T_STORE    = "Store_Silver"
    T_TERR     = "SalesTerritory_Silver"
    tables = [T_CUSTOMER, T_PERSON, T_STORE, T_TERR]

    if args.snapshot_mode == "per-table":
        dates = resolve_dates_per_table(args.silver_root, tables, args.ingestion_date)
        gold_ing = max(dates.values())
        print("[INFO] Using per-table latest partitions:")
        for t in tables:
            print(f"  - {t}: {dates[t]}")
    else:
        common = resolve_date_common(args.silver_root, tables, args.ingestion_date)
        dates = {t: common for t in tables}
        gold_ing = common
        print(f"[INFO] Using common partition for all tables: {common}")

    path = lambda t: f"{args.silver_root}/{t}/ingestion_date={dates[t]}"
    print(f"[INFO] Reading silver partitions (gold_ingestion_date={gold_ing})")
    cust   = spark.read.parquet(path(T_CUSTOMER))
    terr   = spark.read.parquet(path(T_TERR))
    person = spark.read.parquet(path(T_PERSON))
    store  = spark.read.parquet(path(T_STORE))

    # Build the gold frame via the pure helper
    d = build_dim_customer_frame(cust, person, store, terr, spec["schema"])

    # DQ (PK + required)
    pk = spec.get("primary_key", [])
    req = spec.get("required_not_null", pk)
    dq_basic(d, pk, req, dq_cfg=spec.get("dq"))

    # Write Gold (partitioned by spec.write.partition_by) under gold_ing date
    coalesce_n = int(args.coalesce) if args.coalesce not in (None, "", "None") else None
    out = write_gold(
        df=d,
        dst_root=args.gold_root,
        table_name=spec["name"],         # "DimCustomer"
        ingestion_date=gold_ing,
        coalesce=coalesce_n,
        partition_by=spec.get("write", {}).get("partition_by"),
    )
    print(f"[OK] DimCustomer -> {out}")
    spark.stop()

if __name__ == "__main__":
    main()
