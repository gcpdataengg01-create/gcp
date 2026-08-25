"""Module 10: build fct_sales_line in Spark and write curated Parquet."""

import argparse
import json
import logging
import sys

from google.cloud import firestore
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail_fact.build import build_sales_fact


WATERMARK_COLLECTION = "etl_watermarks"
WATERMARK_DOCUMENT = "retail_db__sales_txn"


def parse_args():
    parser = argparse.ArgumentParser(description="Build Retail ETL fct_sales_line")
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--stage-bucket", required=True)
    parser.add_argument("--curated-bucket", required=True)
    return parser.parse_args()


def update_state(project_id: str, run_id: str, fields: dict) -> None:
    client = firestore.Client(project=project_id)
    ref = client.collection(WATERMARK_COLLECTION).document(WATERMARK_DOCUMENT)
    snapshot = ref.get()
    if not snapshot.exists or snapshot.to_dict().get("run_id") != run_id:
        raise RuntimeError("Cannot update fact state: watermark is not owned by this run")
    ref.update({**fields, "updated_at": firestore.SERVER_TIMESTAMP})


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()

    spark = SparkSession.builder.appName(
        f"retail-etl-build-fact-{args.business_date}"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    clean_path = (
        f"gs://{args.stage_bucket}/entity=sales_clean/"
        f"business_date={args.business_date}/"
    )
    date_path = f"gs://{args.curated_bucket}/entity=dim_date/"
    customer_path = f"gs://{args.curated_bucket}/entity=dim_customer/"
    product_path = f"gs://{args.curated_bucket}/entity=dim_product/"
    fact_path = (
        f"gs://{args.curated_bucket}/entity=fct_sales_line/"
        f"invoice_date={args.business_date}/"
    )

    try:
        clean_sales = spark.read.parquet(clean_path)
        dim_date = spark.read.parquet(date_path)
        dim_customer = spark.read.parquet(customer_path)
        dim_product = spark.read.parquet(product_path)

        fact = build_sales_fact(
            clean_sales,
            dim_date,
            dim_customer,
            dim_product,
            args.business_date,
            args.run_id,
        )

        rows = fact.count()
        duplicate_count = (
            fact.groupBy("invoice", "stock_code")
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )
        if duplicate_count:
            raise RuntimeError("C9-003 pre-publish failure: duplicate fact business key")

        fact.write.mode("overwrite").option("compression", "snappy").parquet(fact_path)

        update_state(
            args.project_id,
            args.run_id,
            {
                "status": "FACT_BUILT",
                "fact_rows": rows,
                "fact_path": fact_path,
            },
        )

        logging.info(
            "MODULE10_FACT_METRIC %s",
            json.dumps(
                {
                    "business_date": args.business_date,
                    "run_id": args.run_id,
                    "fact_rows": rows,
                    "fact_path": fact_path,
                }
            ),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Module 10 fact build failed")
        sys.exit(1)
