import argparse
import json
import logging
import sys
import time

from google.cloud import firestore
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail_cleansing.dedupe import (
    flag_business_key_conflicts,
    remove_exact_duplicates,
    keep_non_exact_duplicates,
    dedupe_business_key,
    keep_business_key_winner,
)

from retail_cleansing.rules import (
    apply_business_rules,
    join_fx_rates,
)

from retail_cleansing.quality import (
    validate_fx_presence,
    evaluate_quarantine_threshold,
)

from retail_cleansing.quarantine import (
    build_quarantine_records,
    split_valid_and_quarantine,
    write_quarantine,
)


ENTITY = "sales_txn"
WATERMARK_COLLECTION = "etl_watermarks"
WATERMARK_DOCUMENT = "retail_db__sales_txn"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Apply C5-C7 entity rules to Retail ETL sales data."
    )

    parser.add_argument(
        "--project-id",
        required=True,
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
        "--stage-bucket",
        required=True,
    )

    parser.add_argument(
        "--quarantine-bucket",
        required=True,
    )

    parser.add_argument(
        "--fx-path",
        required=True,
        help="GCS path containing cached GBP/EUR FX reference data",
    )

    parser.add_argument(
        "--extracted-rows",
        required=False,
        type=int,
        default=None,
        help="Optional override for original raw extracted row count; normally read from Firestore",
    )

    parser.add_argument(
        "--quantity-outlier-threshold",
        type=int,
        default=None,
    )

    parser.add_argument(
        "--line-amount-outlier-threshold",
        type=float,
        default=None,
    )

    parser.add_argument(
        "--prior-quarantined-rows",
        type=int,
        default=None,
        help="Optional override for C1-C4 quarantined rows; normally read from Firestore",
    )

    return parser.parse_args()


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def union_optional(left, right):
    if left is None:
        return right

    if right is None:
        return left

    return left.unionByName(
        right,
        allowMissingColumns=True,
    )


def update_watermark_state(project_id: str, run_id: str, fields: dict) -> None:
    client = firestore.Client(project=project_id)
    ref = client.collection(WATERMARK_COLLECTION).document(WATERMARK_DOCUMENT)
    snapshot = ref.get()
    if not snapshot.exists or snapshot.to_dict().get("run_id") != run_id:
        raise RuntimeError("Cannot update C5-C7 state: watermark is not owned by this run")
    ref.update({**fields, "updated_at": firestore.SERVER_TIMESTAMP})


def get_watermark_state(project_id: str, run_id: str) -> dict:
    client = firestore.Client(project=project_id)
    ref = client.collection(WATERMARK_COLLECTION).document(WATERMARK_DOCUMENT)
    snapshot = ref.get()
    if not snapshot.exists:
        raise RuntimeError("Watermark state missing before C5-C7")
    state = snapshot.to_dict()
    if state.get("run_id") != run_id:
        raise RuntimeError("Watermark is not owned by this C5-C7 run")
    return state


def main():
    configure_logging()

    args = parse_args()

    watermark_state = get_watermark_state(args.project_id, args.run_id)
    extracted_rows_for_gate = (
        args.extracted_rows
        if args.extracted_rows is not None
        else int(watermark_state.get("rows_extracted", 0))
    )
    prior_quarantined_rows = (
        args.prior_quarantined_rows
        if args.prior_quarantined_rows is not None
        else int(watermark_state.get("c1_c4_quarantined_rows", 0))
    )

    spark = (
        SparkSession.builder
        .appName(
            f"retail-etl-c5-c7-{args.business_date}"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    input_path = (
        f"gs://{args.stage_bucket}/"
        f"entity=sales_c1_c4/"
        f"business_date={args.business_date}/"
    )

    output_path = (
        f"gs://{args.stage_bucket}/"
        f"entity=sales_clean/"
        f"business_date={args.business_date}/"
    )

    start_time = time.perf_counter()

    try:
        logging.info(
            "Reading C1-C4 stage: %s",
            input_path,
        )

        df = (
            spark.read
            .parquet(input_path)
        )

        input_rows = df.count()

        if input_rows == 0:
            raise RuntimeError(
                "C5-C7 received zero input rows."
            )

        # ====================================================
        # C5-001
        # Exact duplicate removal
        # ====================================================

        exact_flagged_df = (
            remove_exact_duplicates(df)
        )

        exact_duplicates_removed = (
            exact_flagged_df
            .filter(
                F.col(
                    "_dq_c5_exact_duplicate"
                )
            )
            .count()
        )

        df = keep_non_exact_duplicates(
            exact_flagged_df
        )

        # ====================================================
        # C5-003
        # Conflicting business-key duplicates
        # ====================================================

        conflict_df = (
            flag_business_key_conflicts(
                df
            )
        )

        c5_conflicting_source_df = (
            conflict_df.filter(
                F.coalesce(
                    F.col("_dq_c5_conflict"),
                    F.lit(False),
                )
            )
        )

        c5_conflicting_rows = (
            c5_conflicting_source_df.count()
        )

        c5_quarantine_df = (
            build_quarantine_records(
                df=conflict_df,
                run_id=args.run_id,
                business_date=args.business_date,
            )
        )

        # Conflicts must not continue through the fact path.
        df = conflict_df.filter(
            ~F.coalesce(
                F.col("_dq_c5_conflict"),
                F.lit(False),
            )
        )

        # ====================================================
        # C5-002
        # Deterministic (invoice, stock_code) deduplication
        # ====================================================

        ranked_df = (
            dedupe_business_key(df)
        )

        business_duplicates_removed = (
            ranked_df
            .filter(
                F.col(
                    "_dq_c5_business_duplicate"
                )
            )
            .count()
        )

        df = keep_business_key_winner(
            ranked_df
        )

        # ====================================================
        # C6 + non-FX C7 rules
        # ====================================================

        df = apply_business_rules(
            df,
            quantity_abs_threshold=(
                args.quantity_outlier_threshold
            ),
            line_amount_abs_threshold=(
                args.line_amount_outlier_threshold
            ),
        )

        # ====================================================
        # C7-008 / C7-009
        # GBP -> EUR FX
        # ====================================================

        logging.info(
            "Reading FX reference: %s",
            args.fx_path,
        )

        fx_df = (
            spark.read
            .parquet(args.fx_path)
        )

        df = join_fx_rates(
            sales_df=df,
            fx_df=fx_df,
        )

        # Missing FX is FAIL_JOB, not quarantine.
        validate_fx_presence(df)

        # ====================================================
        # C7 Quarantine
        # ====================================================

        valid_df, c7_rejected_source_df = (
            split_valid_and_quarantine(
                df
            )
        )

        c7_rejected_rows = (
            c7_rejected_source_df.count()
        )

        c7_quarantine_df = (
            build_quarantine_records(
                df=df,
                run_id=args.run_id,
                business_date=args.business_date,
            )
        )

        # Combine C5 conflicts and C7 rejected rule records.
        quarantine_df = union_optional(
            c5_quarantine_df,
            c7_quarantine_df,
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
                layer="c5_c7",
            )

        # ====================================================
        # Count DISTINCT source rows quarantined.
        #
        # One source row can violate several rules, so rule
        # record count must not be used for the 2% threshold.
        # ====================================================

        c5_ids = (
            c5_conflicting_source_df
            .select("txn_id")
        )

        c7_ids = (
            c7_rejected_source_df
            .select("txn_id")
        )

        c5_c7_quarantined_rows = (
            c5_ids
            .unionByName(c7_ids)
            .distinct()
            .count()
        )

        quarantined_source_rows = (
            prior_quarantined_rows
            + c5_c7_quarantined_rows
        )

        quarantine_metrics = (
            evaluate_quarantine_threshold(
                extracted_rows=(
                    extracted_rows_for_gate
                ),
                quarantined_source_rows=(
                    quarantined_source_rows
                ),
            )
        )

        logging.info(
            "MODULE8_QUARANTINE_METRIC %s",
            json.dumps(
                quarantine_metrics
            ),
        )

        # Module 8 records the metric.
        # The final publish gate is enforced in Module 10.

        # ====================================================
        # Write clean stage
        # ====================================================

        valid_df = (
            valid_df
            .withColumn(
                "_entity_run_id",
                F.lit(args.run_id),
            )
            .withColumn(
                "_entity_processed_at",
                F.current_timestamp(),
            )
        )

        clean_rows = valid_df.count()

        (
            valid_df.write
            .mode("overwrite")
            .option(
                "compression",
                "snappy",
            )
            .parquet(output_path)
        )

        elapsed_seconds = (
            time.perf_counter()
            - start_time
        )

        metrics = {
            "event": "spark_entity_c5_c7_complete",
            "business_date": (
                args.business_date
            ),
            "run_id": args.run_id,
            "input_rows": input_rows,
            "exact_duplicates_removed": (
                exact_duplicates_removed
            ),
            "business_duplicates_removed": (
                business_duplicates_removed
            ),
            "c5_conflicting_rows": (
                c5_conflicting_rows
            ),
            "c7_rejected_rows": (
                c7_rejected_rows
            ),
            "quarantined_source_rows": (
                quarantined_source_rows
            ),
            "quarantine_rule_records": (
                quarantine_rule_records
            ),
            "clean_rows": clean_rows,
            "output_path": output_path,
            "elapsed_seconds": round(
                elapsed_seconds,
                3,
            ),
            "prior_quarantined_rows": (
                prior_quarantined_rows
            ),

            "c5_c7_quarantined_rows": (
                c5_c7_quarantined_rows
            ),
        }

        logging.info(
            "MODULE8_C5_C7_METRIC %s",
            json.dumps(metrics),
        )

        deliberately_excluded_rows = (
            exact_duplicates_removed + business_duplicates_removed
        )

        repairs_per_rule = dict(watermark_state.get("repairs_per_rule", {}))
        repairs_per_rule.update({
            "C5-001": exact_duplicates_removed,
            "C5-002": business_duplicates_removed,
        })

        update_watermark_state(
            args.project_id,
            args.run_id,
            {
                "status": "C5_C7_COMPLETE",
                "c5_c7_input_rows": input_rows,
                "c5_c7_quarantined_rows": c5_c7_quarantined_rows,
                "quarantined_rows_total": quarantined_source_rows,
                "deliberately_excluded_rows": deliberately_excluded_rows,
                "clean_rows": clean_rows,
                "quarantine_rate": quarantine_metrics["quarantine_rate"],
                "repairs_per_rule": repairs_per_rule,
            },
        )

    except Exception:
        logging.exception(
            "C5-C7 entity job failed."
        )

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)