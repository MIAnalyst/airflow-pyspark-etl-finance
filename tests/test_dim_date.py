from libs.dim_date_lib import build_dim_date_df

def test_row_count_and_uniqueness(spark):
    start, end = "2011-01-01", "2011-01-31"
    df = build_dim_date_df(spark, start, end)
    n = df.count()
    assert n == 31
    assert df.select("DateKey").distinct().count() == n

def test_boundaries(spark):
    start, end = "2011-01-01", "2011-01-05"
    df = build_dim_date_df(spark, start, end)
    min_dk = df.agg({"DateKey":"min"}).first()[0]
    max_dk = df.agg({"DateKey":"max"}).first()[0]
    assert min_dk == 20110101
    assert max_dk == 20110105

def test_no_nulls_in_key_columns(spark):
    start, end = "2011-02-01", "2011-02-10"
    df = build_dim_date_df(spark, start, end)
    for col in ["Date","DateKey","Year","Month","Day"]:
        assert df.filter(df[col].isNull()).count() == 0
