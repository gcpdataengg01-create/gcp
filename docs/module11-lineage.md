# Module 11 - Lineage Notes

The assignment requires reporting which lineage is captured automatically and which Dataproc/Spark lineage needs explicit instrumentation or documentation.

## Expected automatic lineage

After deployment, verify in Dataplex/BigQuery that warehouse-native operations are captured for:

- GCS-curated Parquet loaded into BigQuery staging tables by BigQuery load jobs.
- BigQuery staging-to-curated `MERGE` publish operations.
- Curated fact to `semantic.v_sales` view relationships.

These are warehouse-native operations and are expected to be visible through Google Cloud lineage integrations when enabled for the project. This expectation must be verified during the final GCP run; it is not treated as evidence until the deployed lineage UI/API shows the relationship.

## Spark/GCS lineage requiring explicit evidence

The PySpark path crosses Cloud SQL and multiple GCS prefixes:

`Cloud SQL -> raw GCS -> C1-C4 stage -> C5-C7 clean stage -> dimensions/fact curated GCS`

This repository does not assume that every Dataproc Serverless file-level edge will be automatically captured. Until deployment proves otherwise, these edges are documented/instrumented through:

- run_id and business_date in every job,
- deterministic GCS input/output paths,
- Firestore batch state,
- structured log metrics emitted by Spark jobs,
- Composer task dependencies added in Module 12.

During final validation, record which Spark edges appear automatically in Dataplex lineage. Any missing Spark/GCS edges should remain explicitly documented and can be instrumented with lineage events if required by the deployed environment.

## Evidence to capture after apply

Capture screenshots/exported evidence showing:

1. Dataplex lake/zones/assets.
2. The C9 DataScan and its five rule results.
3. The `customer_id` policy tag on BigQuery schema fields.
4. The authorized semantic view and absence of direct BI base-table grants.
5. Automatic lineage edges that appear for BigQuery operations.
6. The documented Spark/GCS lineage edges that are not automatically present.
