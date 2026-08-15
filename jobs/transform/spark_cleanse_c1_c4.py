import argparse
import json
import logging
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail_cleansing.normalise import (
    normalise_sales,
    apply_operational_description_rules,
)

from retail_cleansing.normalise import normalise_sales
from retail_cleansing.coerce import apply_type_coercion
from retail_cleansing.standardise import apply_domain_standardisation

from retail_cleansing.quality import (
    DataQualityFailure,
    validate_expected_columns,
    validate_non_empty,
    validate_price_schema,
    validate_row_count_range,
)

from retail_cleansing.quarantine import (
    add_required_field_flag,
    build_quarantine_records,
    split_valid_and_quarantine,
    write_quarantine,
)


SOURCE_SYSTEM = "retail_db"
ENTITY = "sales_txn"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Apply C1-C4 cleansing rules to Retail ETL raw data."
    )

    parser.add_argument(
        "--business-date",
        required=True,
    )

    parser.add_argument(
        "--run-id",
        required=True,
    )

    parser.add_argument(
        "--raw-bucket",
        required=True,
    )

    parser.add_argument(
        "--stage-bucket",
        required=True,
    )

    parser.add_argument(
        "--quarantine-bucket",
        required=True,
    )

    parser.add_argument(
        "--trailing-same-weekday-average",
        type=float,
        default=None,
    )

    return parser.parse_args()


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def main():
    configure_logging()

    args = parse_args()

    spark = (
        SparkSession.builder
        .appName(
            f"retail-etl-c1-c4-{args.business_date}"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw_path = (
        f"gs://{args.raw_bucket}/"
        f"source={SOURCE_SYSTEM}/"
        f"table={ENTITY}/"
        f"business_date={args.business_date}/"
        f"run_id={args.run_id}/"
    )

    # Intermediate output for the C5-C7 entity job.
    stage_path = (
        f"gs://{args.stage_bucket}/"
        f"entity=sales_c1_c4/"
        f"business_date={args.business_date}/"
    )

    start_time = time.perf_counter()

    try:
        logging.info(
            "Reading raw input: %s",
            raw_path,
        )

        raw_df = (
            spark.read
            .parquet(raw_path)
        )

        # ====================================================
        # C1 - Structural validation
        # ====================================================

        validate_expected_columns(
            raw_df
        )

        extracted_rows = validate_non_empty(
            raw_df,
            allow_non_trading_day=False,
        )

        row_count_check = validate_row_count_range(
            current_count=extracted_rows,
            trailing_same_weekday_average=(
                args.trailing_same_weekday_average
            ),
        )

        logging.info(
            "C1_ROW_COUNT_CHECK %s",
            json.dumps(row_count_check),
        )

        if row_count_check["status"] == "HOLD":
            raise DataQualityFailure(
                "C1-002 HOLD: source row count is outside "
                "30%-300% of trailing same-weekday average."
            )

        # ====================================================
        # Configuration
        # ====================================================

        sentinel_values = load_sentinels()

        operational_descriptions = (
            load_operational_descriptions()
        )

        date_formats = load_date_formats()

        country_map = load_country_map()

        stock_code_types = (
            load_stock_code_types()
        )

        # ====================================================
        # C2 - Normalisation
        # ====================================================

        df = normalise_sales(
            raw_df,
            sentinel_values,
        )

        df = apply_operational_description_rules(
            df,
            operational_descriptions,
        )

        df = add_required_field_flag(
            df
        )

        # Missing customer ID remains in the dataset.
        # It is routed to Unknown during dimension processing.
        df = df.withColumn(
            "_dq_c2_anonymous_customer",
            F.col("customer_id").isNull(),
        )

        # ====================================================
        # C3 - Type coercion
        # ====================================================

        df = apply_type_coercion(
            df,
            date_formats,
        )

        validate_price_schema(
            df
        )

        # ====================================================
        # C4 - Domain standardisation
        # ====================================================

        df = apply_domain_standardisation(
            df,
            country_map,
            stock_code_types,
        )

        # ====================================================
        # Quarantine C2-C4 violations
        # ====================================================

        valid_df, rejected_source_df = (
            split_valid_and_quarantine(
                df
            )
        )

        rejected_source_rows = (
            rejected_source_df.count()
        )

        quarantine_df = (
            build_quarantine_records(
                df=df,
                run_id=args.run_id,
                business_date=args.business_date,
            )
        )

        quarantine_rule_records = 0

        if quarantine_df is not None:
            quarantine_rule_records = (
                quarantine_df.count()
            )

            write_quarantine(
                quarantine_df=quarantine_df,
                quarantine_bucket=args.quarantine_bucket,
                business_date=args.business_date,
                run_id=args.run_id,
                layer="c1_c4",
            )

        if (
            rejected_source_rows > 0
            and quarantine_rule_records == 0
        ):
            raise RuntimeError(
                "Rejected rows exist but no quarantine "
                "records were generated."
            )

        # ====================================================
        # Stage output
        #
        # Stage is re-runnable, unlike raw.
        # Replace only this business-date partition.
        # ====================================================

        valid_df = (
            valid_df
            .withColumn(
                "_c1_c4_run_id",
                F.lit(args.run_id),
            )
            .withColumn(
                "_c1_c4_processed_at",
                F.current_timestamp(),
            )
        )

        valid_rows = valid_df.count()

        (
            valid_df.write
            .mode("overwrite")
            .option(
                "compression",
                "snappy",
            )
            .parquet(stage_path)
        )

        elapsed_seconds = (
            time.perf_counter()
            - start_time
        )

        metrics = {
            "event": "spark_cleanse_c1_c4_complete",
            "business_date": args.business_date,
            "run_id": args.run_id,
            "raw_path": raw_path,
            "stage_path": stage_path,
            "extracted_rows": extracted_rows,
            "valid_rows": valid_rows,
            "rejected_source_rows": rejected_source_rows,
            "quarantine_rule_records": (
                quarantine_rule_records
            ),
            "elapsed_seconds": round(
                elapsed_seconds,
                3,
            ),
        }

        logging.info(
            "MODULE8_C1_C4_METRIC %s",
            json.dumps(metrics),
        )

    except Exception:
        logging.exception(
            "C1-C4 cleansing job failed."
        )

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)