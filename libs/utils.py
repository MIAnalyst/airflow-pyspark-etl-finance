# libs/utils.py
# Reusable helpers for Bronze ingestion (CSV -> Parquet) with DQ and optional preprocessing.

from __future__ import annotations
import json, os
from datetime import date
from typing import Tuple, List, Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
    BooleanType, TimestampType, DateType
)
from pyspark.sql import functions as F, types as T
from pyspark.sql.types import DecimalType
import argparse
import pathlib
import re


# --- map simple JSON types to Spark types ---
_SIMPLE_TYPES = {
    "string": StringType(),
    "integer": IntegerType(),
    "long": LongType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "timestamp": TimestampType(),
    "date": DateType(),
}

from pyspark.sql.types import DecimalType

def read_csv_with_schema(
    spark: SparkSession,
    path: str,
    schema: StructType,
    fmt: dict | None = None,
    preproc: dict | None = None,
) -> DataFrame:
    """
    Read CSV with schema. Supports per-table format and optional local preprocessing.
    fmt keys commonly used: sep, header, quote, escape, multiLine
    """
    # UTF-16 safe preprocessing to a UTF-8 temp file if config is present
    src = preprocess_file(path, preproc) if preproc else path

    fmt = fmt or {}
    reader = spark.read.option("mode", "FAILFAST").schema(schema)
    reader = reader.option("header", fmt.get("header", True))
    reader = reader.option("sep", fmt.get("sep", ","))     # delimiter
    reader = reader.option("escape", fmt.get("escape", '"'))
    reader = reader.option("quote", fmt.get("quote", '"'))
    reader = reader.option("multiLine", fmt.get("multiLine", True))
    return reader.csv(src)


def dq_basic(
    df: DataFrame,
    primary_key: List[str],
    required_not_null: List[str],
    dq_cfg: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Fast DQ checks:
      - table not empty
      - required_not_null contain no NULLs
      - primary key uniqueness (if provided)
      - expected_rows ± tolerance (tolerance default 0.0)
    Raises RuntimeError on failure.
    """
    dq_cfg = dq_cfg or {}
    n = df.count()
    if n == 0:
        raise RuntimeError("[DQ FAIL] Input has 0 rows.")

    for c in required_not_null:
        if df.filter(F.col(c).isNull()).limit(1).count() > 0:
            raise RuntimeError(f"[DQ FAIL] Column `{c}` has NULLs but is required.")

    if primary_key:
        dup = (
            df.groupBy([F.col(c) for c in primary_key])
              .count()
              .filter(F.col("count") > 1)
              .limit(1)
              .count()
        )
        if dup > 0:
            raise RuntimeError(f"[DQ FAIL] Duplicate primary key detected on {primary_key}.")

    if "expected_rows" in dq_cfg:
        exp = int(dq_cfg["expected_rows"])
        tol = float(dq_cfg.get("tolerance", 0.0))
        low, high = int(exp * (1 - tol)), int(exp * (1 + tol))
        if not (low <= n <= high):
            raise RuntimeError(f"[DQ FAIL] Row count {n} not within expected {exp} ± {tol*100:.1f}% ({low}..{high}).")

def write_bronze(
    df: DataFrame,
    dst_root: str,
    table_name: str,
    ingestion_date: Optional[str] = None,
    coalesce: Optional[int] = None,
    partition_by: Optional[List[str]] = None,
) -> str:
    """
    Write DataFrame to:
      {dst_root}/{table_name}/ingestion_date=YYYY-MM-DD/
    Optionally partition by columns (e.g., TerritoryID, PersonType).
    """
    day = ingestion_date or str(date.today())
    sanitized = table_name.replace(".", "_")
    out_dir = os.path.join(dst_root, sanitized, f"ingestion_date={day}")

    out_df = df.coalesce(int(coalesce)) if coalesce else df
    writer = out_df.write.mode("overwrite")
    if partition_by:
        real_cols = [c for c in partition_by if c in df.columns]
        if real_cols:
            writer = writer.partitionBy(*real_cols)
    writer.parquet(out_dir)
    return out_dir


def cast_columns_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Cast existing columns to the provided schema's types.
    If a column is missing, create it as NULL with the right type.
    Returns a DataFrame with columns ordered as in the schema.
    """
    for f in schema.fields:
        if f.name in df.columns:
            df = df.withColumn(f.name, F.col(f.name).cast(f.dataType))
        else:
            df = df.withColumn(f.name, F.lit(None).cast(f.dataType))
    return df.select([f.name for f in schema.fields])


def expand_xml_auto(df, prefer_col="Demographics"):
    """
    Expand an XML column into multiple string columns.

    - Picks the XML column (default 'Demographics' or first string column that looks like XML)
    - Cleans BOM, xmlns, any trailing custom markers, and trims
    - Auto-discovers simple <tag>value</tag> tags under a root
    - Extracts each tag via xpath_string as STRING
    - Leaves final typing to cast_columns_to_schema(...)
    """
    # --- pick XML column ---
    xml_col = prefer_col if prefer_col in df.columns else None
    if xml_col is None:
        for c, t in df.dtypes:
            if t == "string" and df.where(F.col(c).rlike(r"<\w+")).limit(1).count() > 0:
                xml_col = c
                break
    if xml_col is None:
        raise ValueError("No XML-looking string column found.")

    # --- clean XML ---
    x = (
        df.withColumn("_xml", F.regexp_replace(F.col(xml_col), r"^\ufeff", ""))   # strip BOM
          .withColumn("_xml", F.regexp_replace(F.col("_xml"), r'\s+xmlns="[^"]+"', ""))  # remove xmlns attr
          .withColumn("_xml", F.regexp_replace(F.col("_xml"), r"&\|\s*$", ""))   # custom row terminator, if any
          .withColumn("_xml", F.trim(F.col("_xml")))
    )

    # --- discover tags & root ---
    rows = [r["_xml"] for r in x.where(F.col("_xml").isNotNull()).select("_xml").collect()]
    roots, tags = set(), set()
    for s in rows:
        m = re.search(r"<\s*(\w+)", s)
        if m:
            roots.add(m.group(1))
        tags.update(re.findall(r"<\s*(\w+)>(?:[^<]*)</\1>", s))
    root = next(iter(roots)) if roots else ""

    if not tags:
        raise ValueError(f"No XML-like tags found in column '{xml_col}'.")
    
    # --- extract each tag as STRING, trim spaces ---
    out = x
    for tag in sorted(tags):
        xp = f"{root}/{tag}/text()" if root else f"*/{tag}/text()"
        # IMPORTANT: pass the path as a Column literal
        val = F.xpath_string(F.col("_xml"), F.lit(xp))
        out = out.withColumn(tag, F.trim(val))

    return out.drop("_xml")




def write_silver(
    df,
    dst_root,
    table_name,
    ingestion_date=None,
    coalesce=None,
    partition_by=None,
    save_mode="overwrite",
):
    import datetime, os
    if ingestion_date is None:
        ingestion_date = datetime.date.today().isoformat()

    out_path = os.path.join(dst_root, table_name, f"ingestion_date={ingestion_date}")

    out_df = df.coalesce(int(coalesce)) if coalesce else df
    writer = out_df.write.mode(save_mode).format("parquet")

    if partition_by:
        real_cols = [c for c in partition_by if c in out_df.columns and c != "ingestion_date"]
        if real_cols:
            writer = writer.partitionBy(*real_cols)

    writer.save(out_path)
    return out_path


def fill_string_nulls(df, schema, exclude_cols, fill_value="UNK"):
    """
    Replace NULLs in STRING columns with fill_value, skipping any in exclude_cols.
    Non-STRING columns are untouched.
    """
    out = df
    for field in schema.fields:
        col_name = field.name
        if (
            col_name in out.columns
            and isinstance(field.dataType, StringType)
            and col_name not in exclude_cols
        ):
            out = out.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(fill_value)))
    return out


def normalize_nulls(df, schema_spec=None):
    """
    Normalize blanks/placeholder tokens to NULL.
    If schema_spec is None (or a StructType), derive string columns from the Spark schema.
    If schema_spec is a list of dicts (from JSON), use that.
    """
    if schema_spec is None or isinstance(schema_spec, StructType):
        # Use Spark schema (post-cast friendly)
        str_cols = [f.name for f in (df.schema.fields if schema_spec is None else schema_spec.fields)
                    if isinstance(f.dataType, StringType)]
    else:
        # JSON-like [{name, type, ...}]
        str_cols = [c["name"] for c in schema_spec
                    if str(c.get("type", "")).lower() in ("string", "varchar", "char")]

    for c in str_cols:
        if c in df.columns:
            s = F.trim(F.col(c).cast("string"))
            s = F.regexp_replace(s, r"^\s+$", "")                      # whitespace -> ""
            s = F.regexp_replace(s, r"^(?i)(null|none|n/?a|unknown|unk)$", "")
            df = df.withColumn(c, F.when(s == "", None).otherwise(s))
    return df



def resolve_ingestion_date(
    bronze_root: str,
    table: str,
    requested: Optional[str] = None,
    *,
    must_exist: bool = False,
) -> str:
    """
    Choose an ingestion_date for a single table.

    - If `requested` is provided and not 'latest', return it (optionally assert the partition exists).
    - Otherwise, pick the latest partition under {bronze_root}/{table}/ingestion_date=YYYY-MM-DD.

    Raises RuntimeError if the table folder / partitions are missing, or
    if must_exist=True and the requested partition does not exist.
    """
    # honor explicit date
    if requested and requested.strip().lower() != "latest":
        day = requested.strip()
        if must_exist:
            p = os.path.join(bronze_root, table, f"ingestion_date={day}")
            if not os.path.isdir(p):
                raise RuntimeError(f"Requested partition not found: {p}")
        return day

    base = os.path.join(bronze_root, table)
    if not os.path.isdir(base):
        raise RuntimeError(f"No bronze table folder found at: {base}")

    parts = [d for d in os.listdir(base) if d.startswith("ingestion_date=")]
    if not parts:
        raise RuntimeError(f"No bronze partitions found at {base}")

    dates = sorted(re.sub(r"^ingestion_date=", "", d) for d in parts)
    return dates[-1]


def load_spec_and_schema(path: str):
    with open(path, "r", encoding="utf-8") as f:
        spec = json.load(f)

    if "schema_ddl" in spec and spec["schema_ddl"]:
        schema = StructType.fromDDL(spec["schema_ddl"])
    elif "schema" in spec and spec["schema"]:
        schema = StructType.fromJson(spec["schema"]) if isinstance(spec["schema"], dict) else StructType.fromDDL(spec["schema"])
    else:
        raise ValueError("Spec must include 'schema_ddl' or 'schema'. Please migrate legacy 'fields'.")
    spec["schema"] = schema
    return spec, schema


# ----------------------------
# Local preprocessing (UTF-16 safe)
# ----------------------------
def preprocess_file(src_path: str, preprocess_cfg: dict) -> str:
    """
    Apply simple find/replace *before* Spark reads the file.
    Useful when the vendor delivers files in UTF-16 with custom tokenization.

    Returns:
        Path (str) to the UTF-8 preprocessed file to be passed into Spark.
    """
    src = pathlib.Path(src_path)
    dst_dir = pathlib.Path("artifacts/preprocessed")
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / src.name

    # Attempt UTF-16 first; if not, fallback to UTF-8 (ignore undecodable bytes).
    try:
        text = src.read_text(encoding="utf-16")
    except UnicodeError:
        text = src.read_text(encoding="utf-8", errors="ignore")

    # Apply configured replacements in order, if provided.
    for old, new in preprocess_cfg.get("replace_pairs", []):
        text = text.replace(old, new)

    # Always write the intermediary file as UTF-8 so Spark can read it cleanly.
    dst.write_text(text, encoding="utf-8")
    return str(dst)



def make_bronze_ingest_parser(
    job_desc: str,
    *,
    default_coalesce: int = 1,
    require_schema_json: bool = True,
    src_help: str = "Path to source file",
    schema_help: str = "Path to schema JSON (schema_ddl or schema)",
) -> argparse.ArgumentParser:
    """
    Build a reusable argparse.ArgumentParser for Bronze ingest jobs.

    Args:
        job_desc: Description shown in `--help` for this specific job.
        default_coalesce: Default number of output files to write (coalesce N).
        require_schema_json: If True, add a required --schema-json flag.
        src_help: Custom help text for the --src flag (lets each job say Store/Customer/etc).
        schema_help: Custom help text for --schema-json.

    Returns:
        An argparse.ArgumentParser ready to parse common Bronze ingest flags.
    """
    # Create the parser with a custom description for this job
    ap = argparse.ArgumentParser(description=job_desc)

    # Required: input CSV path
    ap.add_argument("--src", required=True, help=src_help)

    # Optional: allow disabling the schema flag (some jobs might hardcode schema)
    if require_schema_json:
        ap.add_argument("--schema-json", required=True, help=schema_help)

    # Required: destination root (Bronze layer root folder)
    ap.add_argument("--dst", required=True,
                    help="Bronze root dir (e.g., data/bronze/adventureworks)")

    # Optional: partition date; if omitted, downstream code can default to 'today'
    ap.add_argument("--ingestion-date", default=None,
                    help="YYYY-MM-DD (default: today)")

    # Optional: number of output part files to write; defaults to 1 for demos
    ap.add_argument(
        "--coalesce",
        type=int,
        default=default_coalesce,
        help=f"Number of output files (default {default_coalesce})"
    )

    # Hand the configured parser back to the caller (job) to .parse_args()
    return ap

