# Final pre-deployment fixes

This patch closes the blocking issues identified in the final Modules 1-12 review.

## G-01 raw storage

Raw keeps Autoclass, enables object versioning, and receives a lifecycle rule at bucket creation. The default lifecycle age is 90 days and is configurable with `raw_lifecycle_age_days`. The implementation deliberately does not use a bucket retention lock because Spark committers can create/rename/delete temporary objects during a write. Raw business objects remain logically immutable through unique `run_id` paths plus Spark `errorifexists`.

## Publish quarantine threshold

The Module 10 publish gate now fails when distinct quarantined source rows exceed 2% of extracted rows. Exactly 2% passes, matching the assignment wording "exceed 2%".

## Composer 3

`composer_image_version` is explicitly pinned. Before apply, verify that the configured build is available in the target region:

```powershell
gcloud composer image-versions list --location=asia-south1 --project=diksha-503812
```

Override `composer_image_version` locally if the configured build is not listed. Cloud Composer also explicitly installs `google-cloud-firestore` for DAG control-plane state access.

## Managed Spark Python dependencies

`scripts/package/build_runtime_dependencies.ps1` vendors Firestore and PyYAML into `dist/runtime_deps.zip` without transitive native wheels. `deploy_runtime.ps1` uploads the package and every Dataproc PySpark batch receives it through `python_file_uris`. The dependency ZIP also vendors `google-cloud-bigquery==3.43.0`, which exposes `WRITE_TRUNCATE_DATA` while the managed Spark 2.3 runtime can ship an older BigQuery client. The warehouse loader therefore keeps the explicit `bigquery.WriteDisposition.WRITE_TRUNCATE_DATA` enum.

## G-20 cost view

Terraform creates `ops.v_batch_etl_query_usage` from the region-qualified `INFORMATION_SCHEMA.JOBS_BY_PROJECT` view and filters to jobs labeled `pipeline=batch-etl-retail`.

## FX IAM ownership

The raw-bucket FX object-creator grant is now owned only by the storage Terraform module; the duplicate grant was removed from compute.
