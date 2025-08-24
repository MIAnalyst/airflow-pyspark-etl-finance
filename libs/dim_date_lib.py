from pyspark.sql import functions as F

def build_dim_date_df(spark, start: str, end: str):
    # Generate one row per date, inclusive
    df = (
        spark.range(1)  # just to have a single row to project from
             .select(
                 F.explode(
                     F.sequence(
                         F.to_date(F.lit(start)),
                         F.to_date(F.lit(end))
                     )
                 ).alias("Date")
             )
    )

    return (
        df.withColumn("DateKey",   F.date_format("Date","yyyyMMdd").cast("int"))
          .withColumn("Year",      F.year("Date"))
          .withColumn("Quarter",   F.quarter("Date"))
          .withColumn("Month",     F.month("Date"))
          .withColumn("Week",      F.weekofyear("Date"))
          .withColumn("Day",       F.dayofmonth("Date"))
          .withColumn("DayOfWeek", F.date_format("Date","EEEE"))
          .withColumn("IsWeekend", F.when(F.dayofweek("Date").isin(1,7), F.lit(1)).otherwise(F.lit(0)))
          .withColumn("YearMonth", F.date_format("Date","yyyyMM").cast("int"))
          .withColumn("MonthName", F.date_format("Date","MMMM"))
    )
