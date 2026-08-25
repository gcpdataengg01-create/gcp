"""Module 9: build/update dim_product as SCD Type 2 (C8)."""

import argparse
import json
import logging
import sys

from pyspark.sql import SparkSession

from retail_dimensions.common import materialize_for_overwrite, path_exists
from retail_dimensions.scd2 import (
    PRODUCT_COLUMNS,
    apply_product_scd2,
    product_sentinels,
    product_snapshot,
    validate_scd2_ranges,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Build Retail ETL dim_product SCD2")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--stage-bucket", required=True)
    parser.add_argument("--curated-bucket", required=True)
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    spark = SparkSession.builder.appName(
        f"retail-etl-dim-product-scd2-{args.business_date}"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    input_path = (
        f"gs://{args.stage_bucket}/entity=sales_clean/"
        f"business_date={args.business_date}/"
    )
    output_path = f"gs://{args.curated_bucket}/entity=dim_product/"

    try:
        clean_sales = spark.read.parquet(input_path)
        today = product_snapshot(clean_sales, args.business_date)

        if path_exists(spark, output_path):
            existing = spark.read.parquet(output_path).select(*PRODUCT_COLUMNS)
            result = apply_product_scd2(existing, today, args.business_date)
        else:
            result = product_sentinels(spark).unionByName(today)

        validate_scd2_ranges(result)
        result = materialize_for_overwrite(result)

        if result.filter("product_key = -1").count() != 1:
            raise RuntimeError("dim_product must contain exactly one Unknown (-1) member")
        if result.filter("product_key = -2").count() != 1:
            raise RuntimeError("dim_product must contain exactly one Invalid (-2) member")

        current_count = result.filter("product_key > 0 AND is_current = true").count()
        history_count = result.filter("product_key > 0").count()

        result.write.mode("overwrite").option("compression", "snappy").parquet(output_path)
        logging.info(
            "MODULE9_DIM_PRODUCT_METRIC %s",
            json.dumps(
                {
                    "current_products": current_count,
                    "historical_versions": history_count,
                    "output_path": output_path,
                }
            ),
        )
        result.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("dim_product SCD2 build failed")
        sys.exit(1)
