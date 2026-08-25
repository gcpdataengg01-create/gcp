"""Module 9: build/update Type 1 dim_customer from clean sales."""

import argparse
import json
import logging
import sys

from pyspark.sql import SparkSession

from retail_dimensions.common import materialize_for_overwrite, path_exists
from retail_dimensions.customer import (
    CUSTOMER_COLUMNS,
    customer_sentinels,
    customer_snapshot,
    merge_customer_type1,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Build Retail ETL Type 1 dim_customer")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--stage-bucket", required=True)
    parser.add_argument("--curated-bucket", required=True)
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    spark = SparkSession.builder.appName(
        f"retail-etl-dim-customer-{args.business_date}"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    input_path = (
        f"gs://{args.stage_bucket}/entity=sales_clean/"
        f"business_date={args.business_date}/"
    )
    output_path = f"gs://{args.curated_bucket}/entity=dim_customer/"

    try:
        clean_sales = spark.read.parquet(input_path)
        today = customer_snapshot(clean_sales, args.business_date)

        if path_exists(spark, output_path):
            existing = spark.read.parquet(output_path).select(*CUSTOMER_COLUMNS)
            result = merge_customer_type1(existing, today, args.business_date)
        else:
            result = customer_sentinels(spark).unionByName(today)

        result = materialize_for_overwrite(result)

        if result.filter("customer_key = -1").count() != 1:
            raise RuntimeError("dim_customer must contain exactly one Unknown (-1) member")
        if result.filter("customer_key = -2").count() != 1:
            raise RuntimeError("dim_customer must contain exactly one Invalid (-2) member")

        real_count = result.filter("customer_key > 0").count()
        duplicate_keys = (
            result.filter("customer_key > 0")
            .groupBy("customer_id")
            .count()
            .filter("count > 1")
            .limit(1)
            .count()
        )
        if duplicate_keys:
            raise RuntimeError("dim_customer contains duplicate Customer IDs")

        result.write.mode("overwrite").option("compression", "snappy").parquet(output_path)
        logging.info(
            "MODULE9_DIM_CUSTOMER_METRIC %s",
            json.dumps({"real_customers": real_count, "output_path": output_path}),
        )
        result.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("dim_customer build failed")
        sys.exit(1)
