"""Module 10 BigQuery load, C9 gate, idempotent MERGE, and watermark commit."""

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from google.cloud import bigquery
from google.cloud import firestore

from retail_fact.build import FACT_COLUMNS
from retail_fact.quality import (
    require_pass,
    validate_c9_counts,
    validate_control_total,
    validate_quarantine_rate,
)


WATERMARK_COLLECTION = "etl_watermarks"
WATERMARK_DOCUMENT = "retail_db__sales_txn"
JOB_LABELS = {
    "owner": "data-platform",
    "pipeline": "batch-etl-retail",
    "cost-centre": "data-001",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Load and publish Retail ETL warehouse data")
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--curated-bucket", required=True)
    parser.add_argument("--maximum-bytes-billed", required=True, type=int)
    parser.add_argument("--curated-dataset", default="curated")
    parser.add_argument("--staging-dataset", default="staging")
    parser.add_argument("--ops-dataset", default="ops")
    parser.add_argument(
        "--action",
        choices=["stage", "publish", "commit", "all"],
        default="all",
        help="Run one warehouse lifecycle step or the complete Module 10 flow",
    )
    return parser.parse_args()


def get_state(project_id: str, run_id: str) -> dict:
    client = firestore.Client(project=project_id)
    ref = client.collection(WATERMARK_COLLECTION).document(WATERMARK_DOCUMENT)
    snapshot = ref.get()
    if not snapshot.exists:
        raise RuntimeError("Watermark state does not exist")
    state = snapshot.to_dict()
    if state.get("run_id") != run_id:
        raise RuntimeError(
            f"Watermark belongs to run_id={state.get('run_id')}, not {run_id}"
        )
    return state


@firestore.transactional
def _commit_publish(transaction, ref, run_id: str):
    snapshot = ref.get(transaction=transaction)
    if not snapshot.exists:
        raise RuntimeError("Watermark disappeared before publish commit")
    state = snapshot.to_dict()
    if state.get("run_id") != run_id:
        raise RuntimeError("Watermark ownership changed before publish commit")
    high_watermark = state.get("high_watermark")
    transaction.update(
        ref,
        {
            "status": "PUBLISHED",
            "last_success_wm": high_watermark,
            "published_at": firestore.SERVER_TIMESTAMP,
            "updated_at": firestore.SERVER_TIMESTAMP,
        },
    )


def commit_publish_state(project_id: str, run_id: str) -> None:
    client = firestore.Client(project=project_id)
    ref = client.collection(WATERMARK_COLLECTION).document(WATERMARK_DOCUMENT)
    transaction = client.transaction()
    _commit_publish(transaction, ref, run_id)


def query_config(maximum_bytes_billed: int, business_date: str | None = None):
    parameters = []
    if business_date is not None:
        parameters.append(
            bigquery.ScalarQueryParameter("business_date", "DATE", business_date)
        )
    return bigquery.QueryJobConfig(
        maximum_bytes_billed=maximum_bytes_billed,
        labels=JOB_LABELS,
        query_parameters=parameters,
        use_legacy_sql=False,
    )


def scalar_query(client, sql: str, maximum_bytes_billed: int, business_date=None):
    row = next(
        iter(
            client.query(
                sql,
                job_config=query_config(maximum_bytes_billed, business_date),
            ).result()
        )
    )
    return row[0]


def load_parquet(client, uri: str, table_id: str):
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        # Preserve the explicit Terraform-managed schema, including policy tags.
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE_DATA,
        labels=JOB_LABELS,
    )
    client.load_table_from_uri(uri, table_id, job_config=config).result()


def create_run_staging_table(client, curated_fact_id: str, staging_table_id: str):
    target = client.get_table(curated_fact_id)
    client.delete_table(staging_table_id, not_found_ok=True)
    table = bigquery.Table(staging_table_id, schema=target.schema)
    table.expires = datetime.now(timezone.utc) + timedelta(days=14)
    client.create_table(table)


def merge_sql(project: str, curated_dataset: str, staging_table_id: str) -> str:
    target = f"`{project}.{curated_dataset}.fct_sales_line`"
    source = f"`{staging_table_id}`"
    key_columns = {"invoice", "stock_code", "invoice_date_local"}
    update_columns = [column for column in FACT_COLUMNS if column not in key_columns]
    update_set = ",\n  ".join(f"{column} = src.{column}" for column in update_columns)
    insert_columns = ", ".join(FACT_COLUMNS)
    insert_values = ", ".join(f"src.{column}" for column in FACT_COLUMNS)

    return f"""
MERGE {target} AS tgt
USING {source} AS src
ON tgt.invoice = src.invoice
 AND tgt.stock_code = src.stock_code
 AND tgt.invoice_date_local = @business_date
WHEN MATCHED THEN UPDATE SET
  {update_set}
WHEN NOT MATCHED THEN INSERT ({insert_columns})
VALUES ({insert_values})
WHEN NOT MATCHED BY SOURCE
 AND tgt.invoice_date_local = @business_date THEN DELETE
"""


def run_c9_gate(
    client,
    project: str,
    curated_dataset: str,
    staging_table_id: str,
    business_date: str,
    maximum_bytes_billed: int,
    state: dict,
):
    quoted_staging = f"`{staging_table_id}`"

    rows_out = int(
        scalar_query(
            client,
            f"SELECT COUNT(*) FROM {quoted_staging} WHERE invoice_date_local = @business_date",
            maximum_bytes_billed,
            business_date,
        )
    )

    count_result = validate_c9_counts(
        rows_in=int(state.get("rows_extracted", 0)),
        rows_out=rows_out,
        quarantined_rows=int(state.get("quarantined_rows_total", 0)),
        deliberately_excluded_rows=int(state.get("deliberately_excluded_rows", 0)),
    )
    require_pass(count_result)

    quarantine_result = validate_quarantine_rate(
        rows_in=int(state.get("rows_extracted", 0)),
        quarantined_rows=int(state.get("quarantined_rows_total", 0)),
    )
    require_pass(quarantine_result)

    target_total = scalar_query(
        client,
        f"SELECT ROUND(COALESCE(SUM(line_amount_gbp), 0), 2) FROM {quoted_staging} "
        "WHERE invoice_date_local = @business_date",
        maximum_bytes_billed,
        business_date,
    )
    if "source_control_total" not in state:
        raise RuntimeError("C9-002 cannot run: source_control_total missing from extract state")
    total_result = validate_control_total(state["source_control_total"], target_total)
    require_pass(total_result)

    duplicate_groups = int(
        scalar_query(
            client,
            f"""
SELECT COUNT(*) FROM (
  SELECT invoice, stock_code
  FROM {quoted_staging}
  WHERE invoice_date_local = @business_date
  GROUP BY invoice, stock_code
  HAVING COUNT(*) > 1
)
""",
            maximum_bytes_billed,
            business_date,
        )
    )
    if duplicate_groups:
        raise RuntimeError(f"C9-003 failed: {duplicate_groups} duplicate business-key groups")

    product_orphans = int(
        scalar_query(
            client,
            f"""
SELECT COUNT(*)
FROM {quoted_staging} f
LEFT JOIN `{project}.{curated_dataset}.dim_product` p
  ON f.product_key = p.product_key
WHERE p.product_key IS NULL
  AND f.product_key NOT IN (-1, -2)
""",
            maximum_bytes_billed,
        )
    )
    customer_orphans = int(
        scalar_query(
            client,
            f"""
SELECT COUNT(*)
FROM {quoted_staging} f
LEFT JOIN `{project}.{curated_dataset}.dim_customer` c
  ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL
  AND f.customer_key NOT IN (-1, -2)
""",
            maximum_bytes_billed,
        )
    )
    if product_orphans or customer_orphans:
        raise RuntimeError(
            "C9-004 failed: "
            f"product_orphans={product_orphans}, customer_orphans={customer_orphans}"
        )

    newest_date = scalar_query(
        client,
        f"SELECT MAX(invoice_date_local) FROM {quoted_staging}",
        maximum_bytes_billed,
    )
    if str(newest_date) != business_date:
        raise RuntimeError(
            f"C9-005 failed: newest_invoice_date={newest_date}, batch_date={business_date}"
        )

    return {
        "QUARANTINE_THRESHOLD": quarantine_result,
        "C9-001": count_result,
        "C9-002": total_result,
        "C9-003": {"passed": True, "duplicate_groups": 0},
        "C9-004": {
            "passed": True,
            "product_orphans": 0,
            "customer_orphans": 0,
        },
        "C9-005": {"passed": True, "newest_invoice_date": business_date},
    }


def upsert_batch_control(
    client,
    project: str,
    ops_dataset: str,
    maximum_bytes_billed: int,
    run_id: str,
    business_date: str,
    status: str,
    state: dict,
    published_rows: int,
    gate_results: dict,
):
    """Persist an idempotent run-control record for Dataplex C9 revalidation."""

    c9_002 = gate_results["C9-002"]
    table_id = f"`{project}.{ops_dataset}.etl_batch_control`"

    sql = f"""
MERGE {table_id} AS tgt
USING (
  SELECT
    @run_id AS run_id,
    @business_date AS business_date,
    @status AS status,
    @rows_extracted AS rows_extracted,
    @fact_rows AS fact_rows,
    @published_rows AS published_rows,
    @quarantined_rows_total AS quarantined_rows_total,
    @deliberately_excluded_rows AS deliberately_excluded_rows,
    @source_control_total AS source_control_total,
    @target_control_total AS target_control_total,
    @control_total_variance AS control_total_variance,
    @c9_001_passed AS c9_001_passed,
    @c9_002_passed AS c9_002_passed,
    @c9_003_passed AS c9_003_passed,
    @c9_004_passed AS c9_004_passed,
    @c9_005_passed AS c9_005_passed
) AS src
ON tgt.run_id = src.run_id
WHEN MATCHED THEN UPDATE SET
  business_date = src.business_date,
  status = src.status,
  rows_extracted = src.rows_extracted,
  fact_rows = src.fact_rows,
  published_rows = src.published_rows,
  quarantined_rows_total = src.quarantined_rows_total,
  deliberately_excluded_rows = src.deliberately_excluded_rows,
  source_control_total = src.source_control_total,
  target_control_total = src.target_control_total,
  control_total_variance = src.control_total_variance,
  c9_001_passed = src.c9_001_passed,
  c9_002_passed = src.c9_002_passed,
  c9_003_passed = src.c9_003_passed,
  c9_004_passed = src.c9_004_passed,
  c9_005_passed = src.c9_005_passed,
  published_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  run_id,
  business_date,
  status,
  rows_extracted,
  fact_rows,
  published_rows,
  quarantined_rows_total,
  deliberately_excluded_rows,
  source_control_total,
  target_control_total,
  control_total_variance,
  c9_001_passed,
  c9_002_passed,
  c9_003_passed,
  c9_004_passed,
  c9_005_passed,
  published_at
) VALUES (
  src.run_id,
  src.business_date,
  src.status,
  src.rows_extracted,
  src.fact_rows,
  src.published_rows,
  src.quarantined_rows_total,
  src.deliberately_excluded_rows,
  src.source_control_total,
  src.target_control_total,
  src.control_total_variance,
  src.c9_001_passed,
  src.c9_002_passed,
  src.c9_003_passed,
  src.c9_004_passed,
  src.c9_005_passed,
  CURRENT_TIMESTAMP()
)
"""

    parameters = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ScalarQueryParameter("business_date", "DATE", business_date),
        bigquery.ScalarQueryParameter("status", "STRING", status),
        bigquery.ScalarQueryParameter("rows_extracted", "INT64", int(state.get("rows_extracted", 0))),
        bigquery.ScalarQueryParameter("fact_rows", "INT64", int(state.get("fact_rows", 0))),
        bigquery.ScalarQueryParameter("published_rows", "INT64", int(published_rows)),
        bigquery.ScalarQueryParameter(
            "quarantined_rows_total", "INT64", int(state.get("quarantined_rows_total", 0))
        ),
        bigquery.ScalarQueryParameter(
            "deliberately_excluded_rows", "INT64", int(state.get("deliberately_excluded_rows", 0))
        ),
        bigquery.ScalarQueryParameter(
            "source_control_total", "NUMERIC", Decimal(c9_002["source_total"])
        ),
        bigquery.ScalarQueryParameter(
            "target_control_total", "NUMERIC", Decimal(c9_002["target_total"])
        ),
        bigquery.ScalarQueryParameter(
            "control_total_variance", "NUMERIC", Decimal(c9_002["variance"])
        ),
        bigquery.ScalarQueryParameter("c9_001_passed", "BOOL", bool(gate_results["C9-001"]["passed"])),
        bigquery.ScalarQueryParameter("c9_002_passed", "BOOL", bool(gate_results["C9-002"]["passed"])),
        bigquery.ScalarQueryParameter("c9_003_passed", "BOOL", bool(gate_results["C9-003"]["passed"])),
        bigquery.ScalarQueryParameter("c9_004_passed", "BOOL", bool(gate_results["C9-004"]["passed"])),
        bigquery.ScalarQueryParameter("c9_005_passed", "BOOL", bool(gate_results["C9-005"]["passed"])),
    ]

    config = bigquery.QueryJobConfig(
        maximum_bytes_billed=maximum_bytes_billed,
        labels=JOB_LABELS,
        query_parameters=parameters,
        use_legacy_sql=False,
    )
    client.query(sql, job_config=config).result()


def update_state(project_id: str, run_id: str, fields: dict) -> None:
    client = firestore.Client(project=project_id)
    ref = client.collection(WATERMARK_COLLECTION).document(WATERMARK_DOCUMENT)
    snapshot = ref.get()
    if not snapshot.exists or snapshot.to_dict().get("run_id") != run_id:
        raise RuntimeError("Cannot update warehouse state: watermark is not owned by this run")
    ref.update({**fields, "updated_at": firestore.SERVER_TIMESTAMP})


def staging_table_id(args) -> str:
    return f"{args.project_id}.{args.staging_dataset}.fct_sales_line_stg"


def curated_fact_id(args) -> str:
    return f"{args.project_id}.{args.curated_dataset}.fct_sales_line"


def load_dimensions_and_stage_fact(client, args) -> str:
    curated_prefix = f"{args.project_id}.{args.curated_dataset}"
    dimension_uris = {
        "dim_date": f"gs://{args.curated_bucket}/entity=dim_date/*.parquet",
        "dim_customer": f"gs://{args.curated_bucket}/entity=dim_customer/*.parquet",
        "dim_product": f"gs://{args.curated_bucket}/entity=dim_product/*.parquet",
    }
    for table_name, uri in dimension_uris.items():
        load_parquet(client, uri, f"{curated_prefix}.{table_name}")

    stage_id = staging_table_id(args)
    fact_uri = (
        f"gs://{args.curated_bucket}/entity=fct_sales_line/"
        f"invoice_date={args.business_date}/*.parquet"
    )
    load_parquet(client, fact_uri, stage_id)
    return stage_id


def run_stage(args):
    state = get_state(args.project_id, args.run_id)
    if state.get("status") not in {
        "FACT_BUILT", "C9_PASSED", "BQ_PUBLISHED", "PUBLISHED"
    }:
        raise RuntimeError(
            "Expected FACT_BUILT before staging, "
            f"got {state.get('status')}"
        )

    client = bigquery.Client(project=args.project_id, location=args.region)
    stage_id = load_dimensions_and_stage_fact(client, args)
    gate_results = run_c9_gate(
        client,
        args.project_id,
        args.curated_dataset,
        stage_id,
        args.business_date,
        args.maximum_bytes_billed,
        state,
    )
    rows = int(
        scalar_query(
            client,
            f"SELECT COUNT(*) FROM `{stage_id}` WHERE invoice_date_local = @business_date",
            args.maximum_bytes_billed,
            args.business_date,
        )
    )
    upsert_batch_control(
        client=client,
        project=args.project_id,
        ops_dataset=args.ops_dataset,
        maximum_bytes_billed=args.maximum_bytes_billed,
        run_id=args.run_id,
        business_date=args.business_date,
        status="C9_PASSED",
        state=state,
        published_rows=rows,
        gate_results=gate_results,
    )
    update_state(
        args.project_id,
        args.run_id,
        {
            "status": "C9_PASSED",
            "bq_staging_table": stage_id,
            "c9_target_control_total": str(gate_results["C9-002"]["target_total"]),
            "c9_control_total_variance": str(gate_results["C9-002"]["variance"]),
            "quarantine_rate": gate_results["QUARANTINE_THRESHOLD"]["quarantine_rate"],
            "quarantine_threshold": gate_results["QUARANTINE_THRESHOLD"]["threshold"],
        },
    )
    logging.info(
        "MODULE10_STAGE_METRIC %s",
        json.dumps(
            {
                "business_date": args.business_date,
                "run_id": args.run_id,
                "staging_rows": rows,
                "c9": gate_results,
            },
            default=str,
        ),
    )
    return gate_results


def run_publish(args):
    state = get_state(args.project_id, args.run_id)
    if state.get("status") not in {"C9_PASSED", "BQ_PUBLISHED", "PUBLISHED"}:
        raise RuntimeError(
            "Expected C9_PASSED before publish, "
            f"got {state.get('status')}"
        )

    client = bigquery.Client(project=args.project_id, location=args.region)
    stage_id = staging_table_id(args)
    gate_results = run_c9_gate(
        client,
        args.project_id,
        args.curated_dataset,
        stage_id,
        args.business_date,
        args.maximum_bytes_billed,
        state,
    )

    client.query(
        merge_sql(args.project_id, args.curated_dataset, stage_id),
        job_config=query_config(args.maximum_bytes_billed, args.business_date),
    ).result()

    target_id = curated_fact_id(args)
    published_rows = int(
        scalar_query(
            client,
            f"SELECT COUNT(*) FROM `{target_id}` WHERE invoice_date_local = @business_date",
            args.maximum_bytes_billed,
            args.business_date,
        )
    )
    if published_rows != int(state.get("fact_rows", -1)):
        raise RuntimeError(
            "Post-MERGE row count mismatch: "
            f"fact_rows={state.get('fact_rows')}, published_rows={published_rows}"
        )

    upsert_batch_control(
        client=client,
        project=args.project_id,
        ops_dataset=args.ops_dataset,
        maximum_bytes_billed=args.maximum_bytes_billed,
        run_id=args.run_id,
        business_date=args.business_date,
        status="BQ_PUBLISHED",
        state=state,
        published_rows=published_rows,
        gate_results=gate_results,
    )
    update_state(
        args.project_id,
        args.run_id,
        {"status": "BQ_PUBLISHED", "published_rows": published_rows},
    )
    logging.info(
        "MODULE10_PUBLISH_METRIC %s",
        json.dumps(
            {
                "business_date": args.business_date,
                "run_id": args.run_id,
                "published_rows": published_rows,
                "c9": gate_results,
            },
            default=str,
        ),
    )
    return gate_results, published_rows


def run_commit(args):
    state = get_state(args.project_id, args.run_id)
    if state.get("status") == "PUBLISHED":
        logging.info("Watermark already committed for run_id=%s", args.run_id)
        return
    if state.get("status") != "BQ_PUBLISHED":
        raise RuntimeError(
            "Expected BQ_PUBLISHED before watermark commit, "
            f"got {state.get('status')}"
        )

    client = bigquery.Client(project=args.project_id, location=args.region)
    target_id = curated_fact_id(args)
    published_rows = int(
        scalar_query(
            client,
            f"SELECT COUNT(*) FROM `{target_id}` WHERE invoice_date_local = @business_date",
            args.maximum_bytes_billed,
            args.business_date,
        )
    )
    if published_rows != int(state.get("fact_rows", -1)):
        raise RuntimeError("Cannot commit watermark: published fact row count changed")

    stage_id = staging_table_id(args)
    gate_results = run_c9_gate(
        client,
        args.project_id,
        args.curated_dataset,
        stage_id,
        args.business_date,
        args.maximum_bytes_billed,
        state,
    )
    commit_publish_state(args.project_id, args.run_id)
    upsert_batch_control(
        client=client,
        project=args.project_id,
        ops_dataset=args.ops_dataset,
        maximum_bytes_billed=args.maximum_bytes_billed,
        run_id=args.run_id,
        business_date=args.business_date,
        status="PUBLISHED",
        state=state,
        published_rows=published_rows,
        gate_results=gate_results,
    )
    logging.info(
        "MODULE10_COMMIT_METRIC %s",
        json.dumps(
            {
                "business_date": args.business_date,
                "run_id": args.run_id,
                "published_rows": published_rows,
            }
        ),
    )


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()

    if args.action == "stage":
        run_stage(args)
    elif args.action == "publish":
        run_publish(args)
    elif args.action == "commit":
        run_commit(args)
    else:
        run_stage(args)
        run_publish(args)
        run_commit(args)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Module 10 BigQuery warehouse action failed")
        sys.exit(1)
