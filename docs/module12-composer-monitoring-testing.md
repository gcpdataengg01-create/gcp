# Module 12 - Composer, Monitoring, Testing and Demonstration

## Scope

Module 12 orchestrates the existing module jobs rather than duplicating their transformation logic. The production DAG is `dags/retail_batch_etl.py` and is parameterized by `business_date` and `run_id`.

The dependency graph is deliberately entity/table based: watermark claim fans out to sales extraction and FX extraction; sales then flows through C1-C4 and C5-C7; date/customer/product dimensions fan out in parallel; the fact waits for all dimensions; BigQuery staging is followed by the independent Dataplex C9 scan, publish, and only then the transactional watermark commit.

No dataset is passed through XCom. Spark inputs/outputs are identified by GCS paths, table/entity names and run IDs.

## Runtime deployment

Terraform creates Cloud Composer, a versioned runtime-code bucket, log-based metrics, five alert policies, and a Monitoring dashboard. After the final Terraform apply, upload the runtime files with `scripts/module12/deploy_runtime.ps1`. The script uploads the shared cleansing/dimension/fact ZIP packages, the managed-Spark runtime dependency ZIP, plus the PostgreSQL JDBC driver and imports the DAG into Composer.

The FX Cloud Run Job is invoked from the DAG with explicit `BUSINESS_DATE` and `RUN_ID` overrides. The implementation writes Snappy Parquet to `raw/reference/fx/requested_date=<date>/` and carries the latest prior ECB/Frankfurter rate for weekends/holidays.

## Required metrics

The final task emits `MODULE12_RUN_METRIC` with `rows_extracted`, `rows_quarantined`, `control_total_variance`, and end-to-end `duration_seconds`. Repair counts are emitted as `MODULE12_REPAIR_METRIC` with `rule_id` labels. Terraform log-based metrics and alert policies consume these records.

Notification channel IDs are intentionally configurable rather than hard-coded. Set `monitoring_notification_channels` in the final environment variables if real email/PagerDuty/SMS channels are required.

## Required test evidence

T-01 is covered by the Module 8 deterministic cleansing unit tests. T-04 is covered by the Module 9 SCD2 historical-change test. Module 10 covers the C9 gate and fact join invariants. Module 12 supplies runtime SQL/evidence helpers for T-02 idempotency, T-03 reconciliation and T-05 join/orphan preservation.

For T-02, capture `sql/module12/idempotency_fingerprint.sql`, rerun the same business date through the same DAG, and capture it again. Row count, GBP control total and sorted-output SHA256 must be identical.

For T-03, `ops.etl_batch_control.control_total_variance` must remain within +/-0.01. For T-05, the Spark fact builder asserts row preservation at dimension joins and `sql/module12/join_preservation.sql` independently proves there are no unexpected orphan keys.

## Seven-date December backfill

`scripts/module12/trigger_backfill.ps1` triggers the normal `retail_batch_etl` DAG for seven source business dates in December 2010. It does not use a separate backfill implementation. Capture the Composer run history and DAG graph as final evidence.

## Final evidence checklist

Capture the unit-test report, SCD2 before/after proof, BigQuery partition/clustering configuration, C9 reconciliation output, Dataplex scan result, Composer DAG graph, seven-run backfill history, second-run idempotency comparison, Monitoring dashboard/alert policies, and the runbook/recovery procedure.
