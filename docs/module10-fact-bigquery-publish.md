# Module 10 - Fact, BigQuery Load and Publish Gate (C9)

## Scope

Module 10 builds `fct_sales_line` in Spark, writes one business-date partition as Snappy Parquet to the curated GCS zone, loads warehouse data into BigQuery through GCS load jobs, runs the C9 publish gate, performs an idempotent partition-pruned `MERGE`, and advances the Firestore success watermark only after publish verification succeeds.

## Fact grain and joins

The fact grain remains one row per `(invoice, stock_code)` at its latest known state for the business date. The Spark fact builder uses left joins to:

- `dim_date` on `invoice_date_local = calendar_date`;
- Type 1 `dim_customer` on `customer_id`;
- SCD2 `dim_product` on `stock_code` plus `invoice_date_local BETWEEN valid_from AND valid_to`.

No ratios are stored in the fact. Financial measures use decimal-compatible types.

## BigQuery design

Terraform creates:

- `curated` dataset with CMEK;
- `staging` dataset with 14-day partition/table expiration and CMEK;
- `curated.dim_date`;
- `curated.dim_customer`;
- `curated.dim_product`;
- `curated.fct_sales_line`, partitioned by `invoice_date_local` and clustered by `(product_key, country_code)`.

Schemas are explicit JSON files under `infra/modules/warehouse/schemas`.

The loader service account receives BigQuery job-user permission plus dataset-scoped editor access. Run-scoped staging fact tables are runtime artifacts, inherit the staging controls, and are deleted after the load attempt.

## Load and publish path

```text
Spark clean sales + dimensions
  -> Spark fct_sales_line
  -> GCS curated Parquet
  -> BigQuery load jobs
  -> run-scoped staging fact table
  -> C9 validation
  -> partition-pruned MERGE
  -> curated.fct_sales_line
  -> Firestore last_success_wm commit
```

The primary warehouse load is a BigQuery GCS load job, not `INSERT ... SELECT`.

## C9 gate

- **C9-001**: `rows_in = rows_out + quarantined + deliberately_excluded`.
- **C9-002**: GBP control total matches the source total within +/-0.01.
- **C9-003**: no duplicate `(invoice, stock_code)` groups.
- **C9-004**: no product/customer orphan keys other than `-1` and `-2`.
- **C9-005**: newest staged `invoice_date_local` equals the business date.

Module 7 now records the measured PostgreSQL source control total in Firestore. Module 8 records quarantine and deterministic duplicate-removal counts there. This gives Module 10 a durable, run-owned source for C9-001/C9-002 instead of inventing reconciliation values.

## Idempotency

The MERGE is constrained to `@business_date`. It updates existing business keys, inserts new keys, and deletes rows in the same date partition that disappeared from the rerun source. This prevents stale rows after a corrected rerun while pruning historical partitions.

## BigQuery cost guardrail

`maximum_bytes_billed` is configured through Terraform/environment input and passed to every query job. It is not repeated as a hard-coded value throughout the pipeline.

## Validation

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\module10\validate_module10.ps1
python -m pytest .\tests\module10 -v
powershell -ExecutionPolicy Bypass -File .\scripts\package\build_fact_package.ps1
```

Spark/GCP execution can remain deferred until the final cloud validation cycle. Terraform should be initialized/validated before the final apply.
