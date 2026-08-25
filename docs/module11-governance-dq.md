# Module 11 - Dataplex Governance and Data Quality

## Scope

Module 11 implements the assignment governance controls around the data produced by Modules 7-10:

- Dataplex lake, zones, and assets for the four GCS zones plus BigQuery staging, operational-control, curated, and semantic datasets.
- A version-controlled taxonomy/policy tag for pseudonymous `customer_id` columns.
- An on-demand Dataplex data-quality scan that independently rechecks C9-001 through C9-005 after publish.
- A semantic `v_sales` authorized view as the BI-facing access path.
- Lineage documentation that separates automatically captured warehouse lineage from Spark/GCS lineage that requires explicit instrumentation/evidence.

## Dataplex registration

The Dataplex layout is intentionally aligned to the ETL layers:

- `landing` RAW zone: raw and quarantine GCS buckets.
- `processing` RAW zone: stage GCS plus BigQuery staging and ops datasets.
- `curated` CURATED zone: curated GCS, curated BigQuery, and semantic BigQuery datasets.

Discovery is disabled on all assets. Production schemas are defined explicitly in source control, so an uncontrolled crawler is not used to infer warehouse schemas.

## Policy tags

`infra/modules/governance_policy` creates the fine-grained taxonomy and the `Pseudonymous Customer Identifier` policy tag. Module 10 warehouse schemas consume that policy-tag resource name through `templatefile()` and attach it to `customer_id` in both `dim_customer` and `fct_sales_line`.

The semantic view intentionally does not expose `customer_id`. BI principals are optionally supplied through `bi_reader_members` and are granted access to the semantic dataset only, plus `bigquery.jobUser` so they can execute queries.

## Independent C9 data-quality scan

The on-demand scan is created against `curated.fct_sales_line`. Module 10 persists a deterministic `ops.etl_batch_control` record before/after watermark commit so Dataplex can re-evaluate run-level control values without relying on in-memory Spark assertions.

Rules:

- C9-001: independently recomputes rows-in reconciliation from persisted control counts.
- C9-002: independently evaluates the persisted source/target GBP totals and +/-0.01 tolerance.
- C9-003: recomputes duplicate `(invoice, stock_code)` groups directly from the published fact.
- C9-004: recomputes product/customer orphan checks against curated dimensions, allowing only reserved `-1` and `-2` keys.
- C9-005: compares the maximum published invoice date with the latest published control business date.

The scan is `on_demand`; Module 12 orchestration can trigger it after the warehouse publish task rather than running it on an unrelated wall-clock schedule.

## Deployment note

No Module 11 resources should be created manually in the Google Cloud Console. They are Terraform-managed and should be deployed only during the final environment apply.
