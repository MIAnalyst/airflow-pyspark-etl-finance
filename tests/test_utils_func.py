# tests/unit/test_utils_func.py
"""
Unit tests for data-changing helpers in libs/utils.py.

We focus on:
- JSON spec parsing (types, decimals, metadata)
- CSV reading with preprocessing
- Core DQ rules (row count, not-null, PK uniqueness, tolerance)
- Writers (bronze/silver) producing expected paths/partitions
- Casting + column ordering
- XML expansion (happy/negative paths)
- Null normalization and string-null filling

Notes:
- Use the shared `spark` fixture from tests/conftest.py
- Use chispa for tidy DataFrame equality when needed
"""

import os
import json
import pathlib
import pytest
from pyspark.sql import functions as F, types as T

from libs.utils import (
    load_table_spec,
    read_csv_with_schema,
    dq_basic,
    write_bronze,
    cast_columns_to_schema,
    expand_xml_auto,
    write_silver,
    fill_string_nulls,
    normalize_nulls,
)

# ---------------------------------------------------------------------------
# load_table_spec
# ---------------------------------------------------------------------------

def test_load_table_spec_parses_decimal(tmp_path: pathlib.Path) -> None:
    """
    It should parse decimal(precision,scale) correctly and preserve metadata
    like name, primary_key, and dq settings.
    """
    spec = {
        "name": "T",
        "fields": [
            {"name": "A", "type": "integer",       "nullable": False},
            {"name": "B", "type": "decimal(19,4)", "nullable": True},
            {"name": "C", "type": "string",        "nullable": True},
        ],
        "primary_key": ["A"],
        "dq": {"expected_rows": 2, "tolerance": 0.1},
    }
    p = tmp_path / "spec.json"
    p.write_text(json.dumps(spec))

    out = load_table_spec(str(p))

    # schema + decimal check
    assert out["name"] == "T"
    assert [f.name for f in out["schema"].fields] == ["A", "B", "C"]
    assert isinstance(out["schema"]["B"].dataType, T.DecimalType)

    # metadata preserved
    assert out["primary_key"] == ["A"]
    assert out["dq"]["expected_rows"] == 2


def test_load_table_spec_unknown_type_raises(tmp_path: pathlib.Path) -> None:
    """
    Unknown types should surface a clear error. We accept either KeyError
    (current behavior) or ValueError (if you upgrade the guard).
    """
    spec = {"name": "T", "fields": [{"name": "A", "type": "weirdtype", "nullable": True}]}
    p = tmp_path / "spec.json"
    p.write_text(json.dumps(spec))

    with pytest.raises((KeyError, ValueError)):
        load_table_spec(str(p))


# ---------------------------------------------------------------------------
# read_csv_with_schema
# ---------------------------------------------------------------------------

@pytest.mark.spark
def test_read_csv_with_schema_with_preprocess(tmp_path: pathlib.Path, spark) -> None:
    """
    Preprocess should convert +| -> \t and strip &|, then CSV reader should
    honor the provided format options (sep, header, multiLine).
    """
    # raw file uses +| as field sep, &| as trailing marker to remove
    raw = tmp_path / "x.csv"
    raw.write_text("1+|A&|\n2+|B&|\n")

    schema = T.StructType(
        [T.StructField("id", T.IntegerType(), True), T.StructField("val", T.StringType(), True)]
    )
    fmt = {"sep": "\t", "header": False, "multiLine": False}
    pre = {"replace_pairs": [["+|", "\t"], ["&|", ""]]}

    df = read_csv_with_schema(spark, str(raw), schema, fmt=fmt, preproc=pre)

    # Expect two rows parsed correctly
    out = [tuple(r) for r in df.collect()]
    assert out == [(1, "A"), (2, "B")]


# ---------------------------------------------------------------------------
# dq_basic
# ---------------------------------------------------------------------------

@pytest.mark.spark
def test_dq_basic_rowcount_pk_notnull_pass(spark) -> None:
    """
    Happy path: non-empty, no NULLs in required col, PK is unique,
    expected_rows with zero tolerance passes.
    """
    df = spark.createDataFrame([(1, "A"), (2, "B")], "id int, v string")
    dq_basic(df, primary_key=["id"], required_not_null=["id"], dq_cfg={"expected_rows": 2, "tolerance": 0.0})


@pytest.mark.spark
def test_dq_basic_empty_raises(spark) -> None:
    """Empty DataFrame should fail the DQ check."""
    df = spark.createDataFrame([], "id int, v string")
    with pytest.raises(RuntimeError):
        dq_basic(df, [], [], {})


@pytest.mark.spark
def test_dq_basic_notnull_raises(spark) -> None:
    """Required-not-null columns must not contain NULLs."""
    df = spark.createDataFrame([(1, "A"), (None, "B")], "id int, v string")
    with pytest.raises(RuntimeError):
        dq_basic(df, [], ["id"], {})


@pytest.mark.spark
def test_dq_basic_pk_duplicate_raises(spark) -> None:
    """Duplicate primary key values should fail."""
    df = spark.createDataFrame([(1, "A"), (1, "B")], "id int, v string")
    with pytest.raises(RuntimeError):
        dq_basic(df, ["id"], [], {})


@pytest.mark.spark
def test_dq_basic_expected_rows_tolerance(spark) -> None:
    """
    expected_rows with tolerance defines an inclusive [low, high] range.
    With exp=2, tol=0.5 -> [1, 3], a count of 1 should pass.
    """
    df = spark.createDataFrame([(1,)], "id int")
    dq_basic(df, [], [], {"expected_rows": 2, "tolerance": 0.5})


# ---------------------------------------------------------------------------
# write_bronze / write_silver
# ---------------------------------------------------------------------------

@pytest.mark.spark
def test_write_bronze_and_silver_paths(tmp_path: pathlib.Path, spark) -> None:
    """
    Writers should create directories under:
      {root}/{table}/ingestion_date=YYYY-MM-DD/
    and respect coalesce/partition_by. The function returns the exact path.
    """
    df = spark.createDataFrame([(1, "X"), (2, "Y")], "id int, v string")

    out_bz = write_bronze(
        df,
        str(tmp_path),
        "T",
        ingestion_date="2025-08-22",
        coalesce=1,
        partition_by=["v"],  # partition by a real column
    )
    assert os.path.isdir(out_bz)
    # Ensure at least one parquet file exists somewhere under the path
    assert any(fname.endswith(".parquet") for _, _, files in os.walk(out_bz) for fname in files)

    out_sv = write_silver(
        df,
        str(tmp_path),
        "U",
        ingestion_date="2025-08-22",
        coalesce=1,
        partition_by=["v"],
    )
    assert os.path.isdir(out_sv)


# ---------------------------------------------------------------------------
# cast_columns_to_schema
# ---------------------------------------------------------------------------

@pytest.mark.spark
def test_cast_columns_to_schema_orders_casts_adds_missing(spark) -> None:
    """
    The function should:
      - reorder columns to match the target schema,
      - cast types where needed,
      - create missing columns as NULLs with correct types,
      - select only target columns (drop extras).
    """
    src = spark.createDataFrame([("1", "A")], "A string, B string")  # A should become int; C is missing
    target = T.StructType(
        [
            T.StructField("A", T.IntegerType(), True),
            T.StructField("C", T.StringType(), True),
            T.StructField("B", T.StringType(), True),
        ]
    )

    out = cast_columns_to_schema(src, target)

    assert out.columns == ["A", "C", "B"]
    assert [f.dataType.simpleString() for f in out.schema.fields] == ["int", "string", "string"]
    rows = [tuple(r) for r in out.collect()]
    assert rows == [(1, None, "A")]


# ---------------------------------------------------------------------------
# expand_xml_auto
# ---------------------------------------------------------------------------

@pytest.mark.spark
def test_expand_xml_auto_extracts_tags(spark) -> None:
    """
    expand_xml_auto should detect the XML column, strip BOM/xmlns,
    and materialize simple <tag>value</tag> nodes as trimmed STRING columns.
    """
    df = spark.createDataFrame(
        [(1, '<root><Gender>M</Gender><Occupation>BM</Occupation></root>')],
        "id int, Demographics string",
    )
    out = expand_xml_auto(df)  # default prefers 'Demographics'
    assert set(["Gender", "Occupation"]).issubset(set(out.columns))

    row = out.select("Gender", "Occupation").first()
    assert row["Gender"] == "M" and row["Occupation"] == "BM"


@pytest.mark.spark
def test_expand_xml_auto_no_xml_raises(spark) -> None:
    """
    When no valid XML content is present in the chosen column,
    the function should raise a ValueError.
    """
    df = spark.createDataFrame([(1, "not xml")], "id int, txt string")
    with pytest.raises(ValueError):
        # Will scan and fail to find tag patterns
        expand_xml_auto(df, prefer_col="txt")


# ---------------------------------------------------------------------------
# fill_string_nulls
# ---------------------------------------------------------------------------

@pytest.mark.spark
def test_fill_string_nulls_with_exclude(spark) -> None:
    """
    Only STRING columns should be filled; excluded columns must be skipped;
    non-string columns must be untouched.
    """
    schema = T.StructType(
        [
            T.StructField("A", T.StringType(), True),
            T.StructField("B", T.StringType(), True),
            T.StructField("C", T.IntegerType(), True),
        ]
    )
    df = spark.createDataFrame([(None, None, None)], "A string, B string, C int")

    out = fill_string_nulls(df, schema, exclude_cols={"A"}, fill_value="UNK")
    row = out.first()

    assert row["A"] is None     # excluded
    assert row["B"] == "UNK"    # filled
    assert row["C"] is None     # non-string untouched


# ---------------------------------------------------------------------------
# normalize_nulls
# ---------------------------------------------------------------------------

@pytest.mark.spark
def test_normalize_nulls_tokens_and_whitespace(spark) -> None:
    """
    Whitespace-only strings and canonical tokens (null/none/n/a/na/unknown/unk)
    should normalize to NULL, case-insensitively.
    """
    df = spark.createDataFrame([("  ", "N/A", "unk", "value")], "a string, b string, c string, d string")
    out = normalize_nulls(df, None)
    r = out.first()
    assert r["a"] is None and r["b"] is None and r["c"] is None and r["d"] == "value"


@pytest.mark.spark
def test_normalize_nulls_with_json_schema_list(spark) -> None:
    """
    When provided a JSON-like schema list, only the fields declared as string
    should be normalized.
    """
    df = spark.createDataFrame([("   ", 5)], "a string, x int")
    json_like = [{"name": "a", "type": "string"}, {"name": "x", "type": "integer"}]

    out = normalize_nulls(df, json_like)
    assert out.first()["a"] is None


