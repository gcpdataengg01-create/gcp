import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

from google.cloud import firestore
from google.cloud import secretmanager

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SOURCE_SYSTEM = "retail_db"
ENTITY = "sales_txn"

WATERMARK_COLLECTION = "etl_watermarks"
WATERMARK_DOCUMENT = "retail_db__sales_txn"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract one business date from Cloud SQL to immutable GCS raw."
    )

    parser.add_argument("--project-id", required=True)
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)

    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", default="5432")
    parser.add_argument("--db-name", required=True)

    parser.add_argument("--db-user-secret-id", required=True)
    parser.add_argument("--db-password-secret-id", required=True)

    parser.add_argument("--raw-bucket", required=True)

    parser.add_argument(
        "--num-partitions",
        type=int,
        default=8,
        choices=[1, 8],
    )

    parser.add_argument(
        "--fetch-size",
        type=int,
        default=10000,
    )

    return parser.parse_args()


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def get_secret(secret_resource_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()

    response = client.access_secret_version(
        request={
            "name": f"{secret_resource_id}/versions/latest"
        }
    )

    return response.payload.data.decode("UTF-8")


def get_business_window(business_date: str):
    parsed_date = datetime.strptime(
        business_date,
        "%Y-%m-%d",
    ).replace(tzinfo=timezone.utc)

    low = parsed_date
    high = parsed_date + timedelta(days=1)

    return low, high


def timestamp_literal(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


@firestore.transactional
def claim_watermark(
    transaction,
    document_ref,
    run_id,
    low,
    high,
):
    snapshot = document_ref.get(transaction=transaction)

    existing = snapshot.to_dict() if snapshot.exists else {}

    existing_status = existing.get("status")
    existing_run_id = existing.get("run_id")

    terminal_statuses = {None, "FAILED", "PUBLISHED"}
    if (
        existing_status not in terminal_statuses
        and existing_run_id
        and existing_run_id != run_id
    ):
        raise RuntimeError(
            "Watermark is already owned by an active run "
            f"run_id={existing_run_id} status={existing_status}"
        )

    payload = {
        "source_system": SOURCE_SYSTEM,
        "entity": ENTITY,
        "low_watermark": low,
        "high_watermark": high,
        "run_id": run_id,
        "status": "RUNNING",
        "rows_extracted": 0,
        "updated_at": firestore.SERVER_TIMESTAMP,
    }

    # Preserve the previously committed warehouse watermark.
    if "last_success_wm" not in existing:
        payload["last_success_wm"] = None

    transaction.set(
        document_ref,
        payload,
        merge=True,
    )


def mark_raw_written(
    document_ref,
    run_id,
    rows_extracted,
    raw_path,
    source_control_total,
):
    snapshot = document_ref.get()

    if not snapshot.exists:
        raise RuntimeError("Watermark document disappeared during extraction.")

    current = snapshot.to_dict()

    if current.get("run_id") != run_id:
        raise RuntimeError(
            "Watermark ownership changed before extract completion."
        )

    document_ref.update(
        {
            "status": "RAW_WRITTEN",
            "rows_extracted": rows_extracted,
            "raw_path": raw_path,
            "source_control_total": str(source_control_total),
            "updated_at": firestore.SERVER_TIMESTAMP,
        }
    )


def mark_failed(document_ref, run_id, error_message):
    try:
        snapshot = document_ref.get()

        if not snapshot.exists:
            return

        current = snapshot.to_dict()

        if current.get("run_id") != run_id:
            return

        document_ref.update(
            {
                "status": "FAILED",
                "error_message": error_message[:2000],
                "updated_at": firestore.SERVER_TIMESTAMP,
            }
        )

    except Exception:
        logging.exception(
            "Unable to update Firestore watermark to FAILED."
        )


def read_bounds(
    spark,
    jdbc_url,
    username,
    password,
    low_sql,
    high_sql,
):
    query = f"""
        (
            SELECT
                MIN(txn_id) AS lo,
                MAX(txn_id) AS hi,
                COUNT(*) AS source_rows,
                ROUND(COALESCE(SUM(quantity * price), 0)::numeric, 2) AS source_control_total
            FROM sales_txn
            WHERE invoice_date >= TIMESTAMP '{low_sql}'
              AND invoice_date <  TIMESTAMP '{high_sql}'
        ) AS bounds
    """

    bounds_df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", username)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    row = bounds_df.first()

    return row["lo"], row["hi"], row["source_rows"], row["source_control_total"]


def extract_sales(
    spark,
    jdbc_url,
    username,
    password,
    low_sql,
    high_sql,
    lo,
    hi,
    num_partitions,
    fetch_size,
):
    bounded_query = f"""
        (
            SELECT *
            FROM sales_txn
            WHERE invoice_date >= TIMESTAMP '{low_sql}'
              AND invoice_date <  TIMESTAMP '{high_sql}'
        ) AS sales
    """

    effective_partitions = num_partitions

    if lo is None or hi is None:
        return None, 0

    if lo == hi:
        effective_partitions = 1

    reader = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", bounded_query)
        .option("user", username)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", str(fetch_size))
    )

    if effective_partitions > 1:
        reader = (
            reader
            .option("partitionColumn", "txn_id")
            .option("lowerBound", str(lo))
            .option("upperBound", str(hi + 1))
            .option("numPartitions", str(effective_partitions))
        )

    df = reader.load()

    return df, effective_partitions


def main():
    configure_logging()
    args = parse_args()

    low, high = get_business_window(args.business_date)

    low_sql = timestamp_literal(low)
    high_sql = timestamp_literal(high)

    logging.info(
        "Starting extraction business_date=%s run_id=%s window=[%s, %s)",
        args.business_date,
        args.run_id,
        low_sql,
        high_sql,
    )

    firestore_client = firestore.Client(
        project=args.project_id
    )

    watermark_ref = (
        firestore_client
        .collection(WATERMARK_COLLECTION)
        .document(WATERMARK_DOCUMENT)
    )

    transaction = firestore_client.transaction()

    claim_watermark(
        transaction,
        watermark_ref,
        args.run_id,
        low,
        high,
    )

    spark = None

    try:
        username = get_secret(
            args.db_user_secret_id
        )

        password = get_secret(
            args.db_password_secret_id
        )

        jdbc_url = (
            f"jdbc:postgresql://"
            f"{args.db_host}:{args.db_port}/"
            f"{args.db_name}"
        )

        spark = (
            SparkSession.builder
            .appName(
                f"retail-etl-extract-{args.business_date}"
            )
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        lo, hi, source_rows, source_control_total = read_bounds(
            spark=spark,
            jdbc_url=jdbc_url,
            username=username,
            password=password,
            low_sql=low_sql,
            high_sql=high_sql,
        )

        logging.info(
            "Source bounds lo=%s hi=%s source_rows=%s source_control_total=%s",
            lo,
            hi,
            source_rows,
            source_control_total,
        )

        if source_rows == 0:
            logging.warning(
                "No source rows found for business_date=%s",
                args.business_date,
            )

            mark_raw_written(
                watermark_ref,
                args.run_id,
                0,
                None,
                "0.00",
            )

            return

        df, effective_partitions = extract_sales(
            spark=spark,
            jdbc_url=jdbc_url,
            username=username,
            password=password,
            low_sql=low_sql,
            high_sql=high_sql,
            lo=lo,
            hi=hi,
            num_partitions=args.num_partitions,
            fetch_size=args.fetch_size,
        )

        raw_path = (
            f"gs://{args.raw_bucket}/"
            f"source={SOURCE_SYSTEM}/"
            f"table={ENTITY}/"
            f"business_date={args.business_date}/"
            f"run_id={args.run_id}/"
        )

        enriched_df = (
            df
            .withColumn(
                "_loaded_at",
                F.current_timestamp(),
            )
            .withColumn(
                "_source",
                F.lit(SOURCE_SYSTEM),
            )
            .withColumn(
                "_batch_id",
                F.lit(args.run_id),
            )
            .withColumn(
                "_ingest_seq",
                F.monotonically_increasing_id(),
            )
            .withColumn(
                "business_date",
                F.lit(args.business_date).cast("date"),
            )
        )

        # Materialize JDBC extraction once so count and write do not
        # independently query Cloud SQL.
        enriched_df.persist(StorageLevel.DISK_ONLY)

        extract_start = time.perf_counter()

        extracted_rows = enriched_df.count()

        extract_elapsed_seconds = (
            time.perf_counter() - extract_start
        )

        if extracted_rows != source_rows:
            raise RuntimeError(
                "JDBC reconciliation failure: "
                f"bounds query count={source_rows}, "
                f"Spark extracted={extracted_rows}"
            )

        write_start = time.perf_counter()

        (
            enriched_df.write
            .mode("errorifexists")
            .option("compression", "snappy")
            .parquet(raw_path)
        )

        write_elapsed_seconds = (
            time.perf_counter() - write_start
        )

        mark_raw_written(
            document_ref=watermark_ref,
            run_id=args.run_id,
            rows_extracted=extracted_rows,
            raw_path=raw_path,
            source_control_total=source_control_total,
        )

        summary = {
            "event": "sales_extract_complete",
            "source_system": SOURCE_SYSTEM,
            "entity": ENTITY,
            "business_date": args.business_date,
            "run_id": args.run_id,
            "low_watermark": low_sql,
            "high_watermark": high_sql,
            "txn_id_lower_bound": lo,
            "txn_id_upper_bound": hi,
            "requested_partitions": args.num_partitions,
            "effective_partitions": effective_partitions,
            "rows_extracted": extracted_rows,
            "source_control_total": str(source_control_total),
            "jdbc_extract_seconds": round(
                extract_elapsed_seconds,
                3,
            ),
            "raw_write_seconds": round(
                write_elapsed_seconds,
                3,
            ),
            "raw_path": raw_path,
        }

        logging.info(
            "MODULE7_METRIC %s",
            json.dumps(summary),
        )

        enriched_df.unpersist()

    except Exception as exc:
        logging.exception(
            "Sales extraction failed."
        )

        mark_failed(
            watermark_ref,
            args.run_id,
            str(exc),
        )

        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)