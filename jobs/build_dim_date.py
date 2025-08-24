# jobs/build_dim_date.py
import argparse, yaml
from pyspark.sql import SparkSession
from libs.dim_date_lib import build_dim_date_df  # <-- reuse logic here

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--start", default="2011-01-01")
    ap.add_argument("--end",   default="2020-12-31")
    return ap.parse_args()

def main():
    args = parse_args()
    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    silver = cfg["paths"]["silver"]

    spark = (
        SparkSession.builder
        .appName("build_dim_date")
        .config("spark.sql.session.timeZone",
                cfg.get("spark", {}).get("conf", {}).get("spark.sql.session.timeZone", "UTC"))
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    # ← call the reusable logic
    df = build_dim_date_df(spark, args.start, args.end)

    # write (I/O belongs in jobs/, not in libs/)
    out = f"{silver}/dim_date"
    (df.write.mode("overwrite").partitionBy("Year").parquet(out))

    print("DIM_DATE_ROWS", df.count())
    print("DIM_DATE_OUT", out)
    spark.stop()

if __name__ == "__main__":
    main()
