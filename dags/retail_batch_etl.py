"""Module 12 Cloud Composer DAG for the retail batch ETL assignment.

Every task is parameterized by the Airflow logical/business date. Data is never
passed through XCom; only control identifiers are templated into task arguments.
"""

import json
import logging
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.operators.dataplex import DataplexRunDataQualityScanOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from google.cloud import firestore

PROJECT_ID = os.environ["RETAIL_PROJECT_ID"]
REGION = os.environ["RETAIL_REGION"]
CODE_BUCKET = os.environ["RETAIL_CODE_BUCKET"]
DATAPROC_STAGING_BUCKET = os.environ["RETAIL_DATAPROC_STAGING_BUCKET"]
DATAPROC_SA = os.environ["RETAIL_DATAPROC_SERVICE_ACCOUNT"]
BQ_LOADER_SA = os.environ["RETAIL_BQ_LOADER_SERVICE_ACCOUNT"]
SUBNETWORK_URI = os.environ["RETAIL_SUBNETWORK_URI"]
RAW_BUCKET = os.environ["RETAIL_RAW_BUCKET"]
STAGE_BUCKET = os.environ["RETAIL_STAGE_BUCKET"]
CURATED_BUCKET = os.environ["RETAIL_CURATED_BUCKET"]
QUARANTINE_BUCKET = os.environ["RETAIL_QUARANTINE_BUCKET"]
DB_HOST = os.environ["RETAIL_DB_HOST"]
DB_NAME = os.environ["RETAIL_DB_NAME"]
DB_USER_SECRET = os.environ["RETAIL_DB_USER_SECRET"]
DB_PASSWORD_SECRET = os.environ["RETAIL_DB_PASSWORD_SECRET"]
FX_JOB_NAME = os.environ["RETAIL_FX_JOB_NAME"]
DATAPLEX_SCAN_ID = os.environ["RETAIL_DATAPLEX_SCAN_ID"]
MAXIMUM_BYTES_BILLED = os.environ["RETAIL_MAXIMUM_BYTES_BILLED"]

BUSINESS_DATE = "{{ dag_run.conf.get('business_date', ds) }}"
RUN_ID = "{{ dag_run.run_id }}"
BATCH_SUFFIX = "{{ ts_nodash | lower | replace('t', '-') | replace('+', '-') }}"

WATERMARK_COLLECTION = "etl_watermarks"
WATERMARK_DOCUMENT = "retail_db__sales_txn"


def _batch(main_file, args, py_files=None, jars=None, service_account=None):
    pyspark_batch = {
        "main_python_file_uri": f"gs://{CODE_BUCKET}/{main_file}",
        "args": args,
    }
    if py_files:
        pyspark_batch["python_file_uris"] = [
            f"gs://{CODE_BUCKET}/{path}" for path in py_files
        ]
    if jars:
        pyspark_batch["jar_file_uris"] = [
            f"gs://{CODE_BUCKET}/{path}" for path in jars
        ]

    return {
        "pyspark_batch": pyspark_batch,
        "environment_config": {
            "execution_config": {
                "service_account": service_account or DATAPROC_SA,
                "subnetwork_uri": SUBNETWORK_URI,
                "staging_bucket": DATAPROC_STAGING_BUCKET,
                "ttl": "7200s",
            }
        },
        "runtime_config": {
            "version": "2.3",
            "properties": {
                "spark.sql.session.timeZone": "UTC",
            },
        },
        "labels": {
            "owner": "data-platform",
            "pipeline": "batch-etl-retail",
            "cost-centre": "data-001",
        },
    }


def claim_watermark(**context):
    business_date = context["dag_run"].conf.get("business_date") or context["ds"]
    run_id = context["dag_run"].run_id
    low = pendulum.parse(business_date, tz="UTC").start_of("day")
    high = low.add(days=1)

    client = firestore.Client(project=PROJECT_ID)
    ref = client.collection(WATERMARK_COLLECTION).document(WATERMARK_DOCUMENT)
    transaction = client.transaction()

    @firestore.transactional
    def _claim(txn):
        snapshot = ref.get(transaction=txn)
        existing = snapshot.to_dict() if snapshot.exists else {}
        status = existing.get("status")
        owner = existing.get("run_id")
        if status not in {None, "FAILED", "PUBLISHED"} and owner and owner != run_id:
            raise RuntimeError(
                f"Watermark already owned by active run_id={owner} status={status}"
            )
        payload = {
            "source_system": "retail_db",
            "entity": "sales_txn",
            "business_date": business_date,
            "low_watermark": low,
            "high_watermark": high,
            "run_id": run_id,
            "status": "CLAIMED",
            "rows_extracted": 0,
            "updated_at": firestore.SERVER_TIMESTAMP,
        }
        if "last_success_wm" not in existing:
            payload["last_success_wm"] = None
        txn.set(ref, payload, merge=True)

    _claim(transaction)


def emit_run_metrics(**context):
    run_id = context["dag_run"].run_id
    client = firestore.Client(project=PROJECT_ID)
    snapshot = client.collection(WATERMARK_COLLECTION).document(WATERMARK_DOCUMENT).get()
    if not snapshot.exists:
        raise RuntimeError("Missing watermark state while emitting Module 12 metrics")
    state = snapshot.to_dict()
    if state.get("run_id") != run_id or state.get("status") != "PUBLISHED":
        raise RuntimeError("Metrics can only be emitted for the successfully published run")

    start = context["dag_run"].start_date
    duration_seconds = max(0.0, (pendulum.now("UTC") - start).total_seconds())
    payload = {
        "business_date": state.get("business_date") or context["ds"],
        "run_id": run_id,
        "rows_extracted": int(state.get("rows_extracted", 0)),
        "rows_quarantined": int(state.get("quarantined_rows_total", 0)),
        "control_total_variance": float(state.get("c9_control_total_variance", 0) or 0),
        "duration_seconds": round(duration_seconds, 3),
    }
    logging.info("MODULE12_RUN_METRIC %s", json.dumps(payload))

    for rule_id, repairs in sorted((state.get("repairs_per_rule") or {}).items()):
        logging.info(
            "MODULE12_REPAIR_METRIC %s",
            json.dumps({"rule_id": rule_id, "repairs": int(repairs)}),
        )


def _warehouse_args(action):
    return [
        "--project-id", PROJECT_ID,
        "--region", REGION,
        "--business-date", BUSINESS_DATE,
        "--run-id", RUN_ID,
        "--curated-bucket", CURATED_BUCKET,
        "--maximum-bytes-billed", MAXIMUM_BYTES_BILLED,
        "--action", action,
    ]


with DAG(
    dag_id="retail_batch_etl",
    description="Table-by-table retail batch ETL on Dataproc Serverless",
    start_date=pendulum.datetime(2009, 1, 1, tz="UTC"),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["retail", "batch-etl", "assignment"],
) as dag:
    start = EmptyOperator(task_id="start")

    claim = PythonOperator(
        task_id="claim_watermark",
        python_callable=claim_watermark,
        do_xcom_push=False,
    )

    extract_sales = DataprocCreateBatchOperator(
        task_id="extract_sales",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"extract-{BATCH_SUFFIX}",
        batch=_batch(
            "jobs/extract/extract_sales.py",
            [
                "--project-id", PROJECT_ID,
                "--business-date", BUSINESS_DATE,
                "--run-id", RUN_ID,
                "--db-host", DB_HOST,
                "--db-name", DB_NAME,
                "--db-user-secret-id", DB_USER_SECRET,
                "--db-password-secret-id", DB_PASSWORD_SECRET,
                "--raw-bucket", RAW_BUCKET,
                "--num-partitions", "8",
                "--fetch-size", "10000",
            ],
            jars=["deps/postgresql.jar"],
        ),
        deferrable=True,
        do_xcom_push=False,
    )

    extract_fx = CloudRunExecuteJobOperator(
        task_id="extract_fx",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=FX_JOB_NAME,
        overrides={
            "container_overrides": [
                {
                    "env": [
                        {"name": "BUSINESS_DATE", "value": BUSINESS_DATE},
                        {"name": "RUN_ID", "value": RUN_ID},
                    ]
                }
            ]
        },
        deferrable=True,
        do_xcom_push=False,
    )

    cleanse_c1_c4 = DataprocCreateBatchOperator(
        task_id="cleanse_c1_c4",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"c1c4-{BATCH_SUFFIX}",
        batch=_batch(
            "jobs/transform/spark_cleanse_c1_c4.py",
            [
                "--project-id", PROJECT_ID,
                "--business-date", BUSINESS_DATE,
                "--run-id", RUN_ID,
                "--raw-bucket", RAW_BUCKET,
                "--stage-bucket", STAGE_BUCKET,
                "--quarantine-bucket", QUARANTINE_BUCKET,
            ],
            py_files=["packages/retail_cleansing.zip"],
        ),
        deferrable=True,
        do_xcom_push=False,
    )

    entity_c5_c7 = DataprocCreateBatchOperator(
        task_id="entity_c5_c7",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"c5c7-{BATCH_SUFFIX}",
        batch=_batch(
            "jobs/entity/spark_entity_c5_c7.py",
            [
                "--project-id", PROJECT_ID,
                "--business-date", BUSINESS_DATE,
                "--run-id", RUN_ID,
                "--stage-bucket", STAGE_BUCKET,
                "--quarantine-bucket", QUARANTINE_BUCKET,
                "--fx-path", f"gs://{RAW_BUCKET}/reference/fx/requested_date={BUSINESS_DATE}/",
            ],
            py_files=["packages/retail_cleansing.zip"],
        ),
        deferrable=True,
        do_xcom_push=False,
    )

    dim_date = DataprocCreateBatchOperator(
        task_id="dim_date",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"date-{BATCH_SUFFIX}",
        batch=_batch(
            "jobs/dimensions/build_date.py",
            ["--curated-bucket", CURATED_BUCKET],
        ),
        deferrable=True,
        do_xcom_push=False,
    )

    dim_customer = DataprocCreateBatchOperator(
        task_id="dim_customer",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"customer-{BATCH_SUFFIX}",
        batch=_batch(
            "jobs/dimensions/build_customer.py",
            [
                "--business-date", BUSINESS_DATE,
                "--stage-bucket", STAGE_BUCKET,
                "--curated-bucket", CURATED_BUCKET,
            ],
            py_files=["packages/retail_dimensions.zip"],
        ),
        deferrable=True,
        do_xcom_push=False,
    )

    dim_product = DataprocCreateBatchOperator(
        task_id="dim_product_scd2",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"product-{BATCH_SUFFIX}",
        batch=_batch(
            "jobs/scd/product_scd2.py",
            [
                "--business-date", BUSINESS_DATE,
                "--stage-bucket", STAGE_BUCKET,
                "--curated-bucket", CURATED_BUCKET,
            ],
            py_files=["packages/retail_dimensions.zip"],
        ),
        deferrable=True,
        do_xcom_push=False,
    )

    build_fact = DataprocCreateBatchOperator(
        task_id="build_fact",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"fact-{BATCH_SUFFIX}",
        batch=_batch(
            "jobs/fact/build_fact.py",
            [
                "--project-id", PROJECT_ID,
                "--business-date", BUSINESS_DATE,
                "--run-id", RUN_ID,
                "--stage-bucket", STAGE_BUCKET,
                "--curated-bucket", CURATED_BUCKET,
            ],
            py_files=["packages/retail_fact.zip"],
        ),
        deferrable=True,
        do_xcom_push=False,
    )

    load_bq_staging = DataprocCreateBatchOperator(
        task_id="load_bq_staging",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"bqstage-{BATCH_SUFFIX}",
        batch=_batch(
            "jobs/warehouse/load_bigquery.py",
            _warehouse_args("stage"),
            py_files=["packages/retail_fact.zip"],
            service_account=BQ_LOADER_SA,
        ),
        deferrable=True,
        do_xcom_push=False,
    )

    dataplex_quality = DataplexRunDataQualityScanOperator(
        task_id="dataplex_quality",
        project_id=PROJECT_ID,
        region=REGION,
        data_scan_id=DATAPLEX_SCAN_ID,
        asynchronous=False,
        fail_on_dq_failure=True,
        result_timeout=1800,
        deferrable=True,
        do_xcom_push=False,
    )

    publish = DataprocCreateBatchOperator(
        task_id="publish",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"publish-{BATCH_SUFFIX}",
        batch=_batch(
            "jobs/warehouse/load_bigquery.py",
            _warehouse_args("publish"),
            py_files=["packages/retail_fact.zip"],
            service_account=BQ_LOADER_SA,
        ),
        deferrable=True,
        do_xcom_push=False,
    )

    commit = DataprocCreateBatchOperator(
        task_id="commit_watermark",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"commit-{BATCH_SUFFIX}",
        batch=_batch(
            "jobs/warehouse/load_bigquery.py",
            _warehouse_args("commit"),
            py_files=["packages/retail_fact.zip"],
            service_account=BQ_LOADER_SA,
        ),
        deferrable=True,
        do_xcom_push=False,
    )

    emit_metrics = PythonOperator(
        task_id="emit_run_metrics",
        python_callable=emit_run_metrics,
        do_xcom_push=False,
    )

    end = EmptyOperator(task_id="end")

    start >> claim
    claim >> [extract_sales, extract_fx]
    extract_sales >> cleanse_c1_c4
    [cleanse_c1_c4, extract_fx] >> entity_c5_c7
    entity_c5_c7 >> [dim_date, dim_customer, dim_product]
    [dim_date, dim_customer, dim_product] >> build_fact
    build_fact >> load_bq_staging >> dataplex_quality >> publish >> commit
    commit >> emit_metrics >> end
