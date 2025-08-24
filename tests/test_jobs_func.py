import pytest
from pyspark.sql import functions as F
from jobs.transform_person_silver import clean_iso_local_date_col
from jobs.transform_customer_silver import apply_customer_silver_rules
from jobs.build_dim_customer import (
    derive_origin_year_col,
    map_business_col,
    band_yearly_income_col,
    add_scd_v1,
)


@pytest.mark.spark
def test_apply_customer_silver_rules_all_cases_and_drops(spark):
    # Rows:
    # 1: Person-only
    # 2: Store-only
    # 3: Both
    # 4: Neither -> UNK
    # 5: Has a wrong preexisting Type that must be overwritten
    df_in = spark.createDataFrame(
        [
            # CustomerID, PersonID, StoreID, Type_in, rowguid, ModifiedDate
            (1, 10,   None,  None,    "rg1", "2020-01-01"),
            (2, None, 20,    None,    "rg2", "2020-01-02"),
            (3, 11,   21,    None,    "rg3", "2020-01-03"),
            (4, None, None,  None,    "rg4", "2020-01-04"),
            (5, 10,   None,  "Store", "rg5", "2020-01-05"),  # wrong on purpose; should become "Person"
        ],
        "CustomerID int, PersonID int, StoreID int, Type string, rowguid string, ModifiedDate string",
    )

    out = apply_customer_silver_rules(df_in)

    # Assert Type derivation (including overwrite on row 5)
    got = {r.CustomerID: r.Type for r in out.select("CustomerID", "Type").collect()}
    assert got == {
        1: "Person",
        2: "Store",
        3: "Both",
        4: "UNK",
        5: "Person",   # overwritten
    }

    # Dropped columns are gone
    assert "rowguid" not in out.columns
    assert "ModifiedDate" not in out.columns

    # Source identifiers remain
    for col in ("CustomerID", "PersonID", "StoreID"):
        assert col in out.columns


@pytest.mark.spark
def test_derive_origin_year_col_variants(spark):
    df = spark.createDataFrame(
        [
            ("Person", "1980-05-10",  None),
            ("Store",  None,          2005),
            ("Both",   "1970-01-01",  1999),
            ("UNK",    None,          None),
        ],
        "Type string, BirthDate string, YearOpened int",
    ).withColumn(
        "oy", derive_origin_year_col(F.col("Type"), F.col("BirthDate"), F.col("YearOpened"))
    )

    got = [r.oy for r in df.select("oy").orderBy("Type").collect()]
    # orderBy("Type") => ["Both","Person","Store","UNK"] => [1999,1980,2005,None]
    assert got == [1999, 1980, 2005, None]


@pytest.mark.spark
def test_map_business_col(spark):
    df = spark.createDataFrame(
        [
            ("Person", "BM",    None),   # -> Occupation -> "Bike Manufacturer"
            ("Person", "Engineer", None),# -> Occupation passthrough
            ("Store",  None,   "BS"),    # -> BusinessType -> "Bike Shop"
            ("Both",   None,   "OS"),    # -> "Outdoor Supplier"
            ("UNK",    None,   None),    # -> None
        ],
        "Type string, Occupation string, BusinessType string",
    ).withColumn(
        "biz", map_business_col(F.col("Type"), F.col("Occupation"), F.col("BusinessType"))
    )

    rows = {r.Type: r.biz for r in df.select("Type", "biz").collect()}
    assert rows["Person"] in ("Bike Manufacturer", "Engineer")  # There are two Person rows; we’ll check set below
    # Build a set for exact check
    got = set([r.biz for r in df.where(F.col("Type") == "Person").select("biz").collect()])
    assert got == {"Bike Manufacturer", "Engineer"}
    assert rows["Store"] == "Bike Shop"
    assert rows["Both"] == "Outdoor Supplier"
    assert rows["UNK"] is None


@pytest.mark.spark
def test_band_yearly_income_col(spark):
    df = spark.createDataFrame(
        [
            ("Store",  20000,   None),           # 0-25000
            ("Store",  75000,   None),           # 50001-75000
            ("Store", 100001,   None),           # 100000+
            ("Both",  50000,   None),            # 25001-50000
            ("Person", None,   "25001-50000"),   # keep existing income
            ("UNK",    None,   None),            # keep as None
        ],
        "Type string, AnnualRevenue int, YearlyIncome string",
    ).withColumn(
        "band", band_yearly_income_col(F.col("Type"), F.col("AnnualRevenue"), F.col("YearlyIncome"))
    )

    got = [r.band for r in df.select("band").collect()]
    assert got == ["0-25000", "50001-75000", "100000+", "25001-50000", "25001-50000", None]


@pytest.mark.spark
def test_add_scd_v1(spark):
    df = spark.createDataFrame([(1,)], "CustomerID int")
    out = add_scd_v1(df, effective_from="2020-01-02")
    row = out.select("effective_from", "effective_to", "is_current").first()
    assert row.effective_from.isoformat() == "2020-01-02"
    assert row.effective_to is None
    assert row.is_current == 1


@pytest.mark.spark
def test_clean_iso_local_date_col_variants(spark):
    base = spark.createDataFrame([
        ("1980-05-10T00:00:00Z",),
        ("1980-05-10 00:00:00+00:00",),
        ("1980-05-10Z",),
        ("1980-05-10",),
        ("",),
        (None,),
        ("not-a-date",),
    ], "raw string")

    # IMPORTANT: pass the column name, not F.col("raw")
    df = base.withColumn("d", clean_iso_local_date_col("raw"))

    got = [(r["raw"], r["d"].isoformat() if r["d"] else None) for r in df.collect()]

    assert ("1980-05-10T00:00:00Z", "1980-05-10") in got
    assert ("1980-05-10 00:00:00+00:00", "1980-05-10") in got
    assert ("1980-05-10Z", "1980-05-10") in got
    assert ("1980-05-10", "1980-05-10") in got
    assert ("", None) in got
    assert (None, None) in got
    assert ("not-a-date", None) in got