"""Module 9: build dim_date in Spark."""

import argparse
import json
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_START_DATE = "2009-01-01"
DEFAULT_END_DATE = "2012-12-31"


def parse_args():
    parser = argparse.ArgumentParser(description="Build Retail ETL dim_date")
    parser.add_argument("--curated-bucket", required=True)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    return parser.parse_args()


def build_date_dimension(spark, start_date: str, end_date: str):
    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")

    dates = spark.sql(
        f"""
        SELECT explode(
            sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
            )
        ) AS calendar_date
        """
    )

    return (
        dates.withColumn(
            "date_key", F.date_format("calendar_date", "yyyyMMdd").cast("int")
        )
        .withColumn("calendar_year", F.year("calendar_date"))
        .withColumn("calendar_quarter", F.quarter("calendar_date"))
        .withColumn("calendar_month", F.month("calendar_date"))
        .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("calendar_date"))
        .withColumn("day_of_month", F.dayofmonth("calendar_date"))
        .withColumn("day_of_week", F.dayofweek("calendar_date"))
        .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("calendar_date").isin(1, 7))
        # No holiday calendar is supplied by the assignment. The business-day
        # baseline is therefore Monday-Friday and can be enriched later if a
        # governed holiday reference is introduced.
        .withColumn("is_business_day", ~F.col("is_weekend"))
        .select(
            "date_key",
            "calendar_date",
            "calendar_year",
            "calendar_quarter",
            "calendar_month",
            "month_name",
            "week_of_year",
            "day_of_month",
            "day_of_week",
            "day_name",
            "is_weekend",
            "is_business_day",
        )
    )


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    spark = SparkSession.builder.appName("retail-etl-dim-date").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = build_date_dimension(spark, args.start_date, args.end_date)
        actual = df.count()
        expected = (
            spark.sql(
                f"SELECT datediff(to_date('{args.end_date}'), "
                f"to_date('{args.start_date}')) + 1 AS n"
            ).first()["n"]
        )
        if actual != expected:
            raise RuntimeError(f"dim_date row count mismatch expected={expected} actual={actual}")

        output_path = f"gs://{args.curated_bucket}/entity=dim_date/"
        df.write.mode("overwrite").option("compression", "snappy").parquet(output_path)
        logging.info(
            "MODULE9_DIM_DATE_METRIC %s",
            json.dumps({"rows": actual, "output_path": output_path}),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("dim_date build failed")
        sys.exit(1)
