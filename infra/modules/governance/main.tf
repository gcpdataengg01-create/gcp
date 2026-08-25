data "google_project" "current" {
  project_id = var.project_id
}

locals {
  lake_name                  = "retail-etl-${var.environment}"
  semantic_dataset_id        = "semantic"
  dataplex_service_agent     = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-dataplex.iam.gserviceaccount.com"
  fact_table_resource        = "//bigquery.googleapis.com/projects/${var.project_id}/datasets/${var.staging_dataset_id}/tables/fct_sales_line_stg"
  batch_control_table        = "`${var.project_id}.${var.ops_dataset_id}.etl_batch_control`"
  curated_product_table      = "`${var.project_id}.${var.curated_dataset_id}.dim_product`"
  curated_customer_table     = "`${var.project_id}.${var.curated_dataset_id}.dim_customer`"
}

# -----------------------------------------------------------------------------
# Dataplex lake / zones / assets
# Discovery is deliberately disabled because production schemas are explicit and
# version-controlled; the assignment prohibits relying on an uncontrolled crawler.
# -----------------------------------------------------------------------------
resource "google_dataplex_lake" "retail" {
  project      = var.project_id
  location     = var.region
  name         = local.lake_name
  display_name = "Retail Batch ETL ${var.environment}"
  description  = "Governance lake for the retail batch ETL assignment."
  labels       = var.labels
}

resource "google_dataplex_zone" "landing" {
  project      = var.project_id
  location     = var.region
  lake         = google_dataplex_lake.retail.name
  name         = "landing"
  type         = "RAW"
  display_name = "Landing and quarantine"

  discovery_spec {
    enabled = false
  }

  resource_spec {
    location_type = "SINGLE_REGION"
  }
}

resource "google_dataplex_zone" "processing" {
  project      = var.project_id
  location     = var.region
  lake         = google_dataplex_lake.retail.name
  name         = "processing"
  type         = "RAW"
  display_name = "Processing and staging"

  discovery_spec {
    enabled = false
  }

  resource_spec {
    location_type = "SINGLE_REGION"
  }
}

resource "google_dataplex_zone" "curated" {
  project      = var.project_id
  location     = var.region
  lake         = google_dataplex_lake.retail.name
  name         = "curated"
  type         = "CURATED"
  display_name = "Curated analytical data"

  discovery_spec {
    enabled = false
  }

  resource_spec {
    location_type = "SINGLE_REGION"
  }
}

resource "google_dataplex_asset" "raw_bucket" {
  project       = var.project_id
  location      = var.region
  lake          = google_dataplex_lake.retail.name
  dataplex_zone = google_dataplex_zone.landing.name
  name          = "raw-gcs"
  display_name  = "Raw GCS"
  labels        = var.labels

  discovery_spec {
    enabled = false
  }

  resource_spec {
    name             = "projects/${var.project_id}/buckets/${var.raw_bucket_name}"
    type             = "STORAGE_BUCKET"
    read_access_mode = "DIRECT"
  }
}

resource "google_dataplex_asset" "quarantine_bucket" {
  project       = var.project_id
  location      = var.region
  lake          = google_dataplex_lake.retail.name
  dataplex_zone = google_dataplex_zone.landing.name
  name          = "quarantine-gcs"
  display_name  = "Quarantine GCS"
  labels        = var.labels

  discovery_spec {
    enabled = false
  }

  resource_spec {
    name             = "projects/${var.project_id}/buckets/${var.quarantine_bucket_name}"
    type             = "STORAGE_BUCKET"
    read_access_mode = "DIRECT"
  }
}

resource "google_dataplex_asset" "stage_bucket" {
  project       = var.project_id
  location      = var.region
  lake          = google_dataplex_lake.retail.name
  dataplex_zone = google_dataplex_zone.processing.name
  name          = "stage-gcs"
  display_name  = "Stage GCS"
  labels        = var.labels

  discovery_spec {
    enabled = false
  }

  resource_spec {
    name             = "projects/${var.project_id}/buckets/${var.stage_bucket_name}"
    type             = "STORAGE_BUCKET"
    read_access_mode = "DIRECT"
  }
}

resource "google_dataplex_asset" "staging_bigquery" {
  project       = var.project_id
  location      = var.region
  lake          = google_dataplex_lake.retail.name
  dataplex_zone = google_dataplex_zone.processing.name
  name          = "staging-bigquery"
  display_name  = "Staging BigQuery"
  labels        = var.labels

  discovery_spec {
    enabled = false
  }

  resource_spec {
    name = "projects/${var.project_id}/datasets/${var.staging_dataset_id}"
    type = "BIGQUERY_DATASET"
  }
}

resource "google_dataplex_asset" "ops_bigquery" {
  project       = var.project_id
  location      = var.region
  lake          = google_dataplex_lake.retail.name
  dataplex_zone = google_dataplex_zone.processing.name
  name          = "ops-bigquery"
  display_name  = "Operational controls BigQuery"
  labels        = var.labels

  discovery_spec {
    enabled = false
  }

  resource_spec {
    name = "projects/${var.project_id}/datasets/${var.ops_dataset_id}"
    type = "BIGQUERY_DATASET"
  }
}

resource "google_dataplex_asset" "curated_bucket" {
  project       = var.project_id
  location      = var.region
  lake          = google_dataplex_lake.retail.name
  dataplex_zone = google_dataplex_zone.curated.name
  name          = "curated-gcs"
  display_name  = "Curated GCS"
  labels        = var.labels

  discovery_spec {
    enabled = false
  }

  resource_spec {
    name             = "projects/${var.project_id}/buckets/${var.curated_bucket_name}"
    type             = "STORAGE_BUCKET"
    read_access_mode = "DIRECT"
  }
}

resource "google_dataplex_asset" "curated_bigquery" {
  project       = var.project_id
  location      = var.region
  lake          = google_dataplex_lake.retail.name
  dataplex_zone = google_dataplex_zone.curated.name
  name          = "curated-bigquery"
  display_name  = "Curated BigQuery"
  labels        = var.labels

  discovery_spec {
    enabled = false
  }

  resource_spec {
    name = "projects/${var.project_id}/datasets/${var.curated_dataset_id}"
    type = "BIGQUERY_DATASET"
  }
}

# -----------------------------------------------------------------------------
# Semantic dataset + authorized view. BI readers get access only to semantic.
# -----------------------------------------------------------------------------
resource "google_bigquery_dataset" "semantic" {
  project    = var.project_id
  dataset_id = local.semantic_dataset_id
  location   = var.region

  delete_contents_on_destroy = var.environment == "dev"

  default_encryption_configuration {
    kms_key_name = var.kms_key_id
  }

  labels = var.labels
}

resource "google_bigquery_table" "sales_view" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.semantic.dataset_id
  table_id   = "v_sales"

  deletion_protection = var.environment != "dev"

  view {
    use_legacy_sql = false
    query = <<-SQL
      SELECT
        invoice_date_local,
        country_code,
        product_key,
        quantity,
        line_amount_gbp,
        line_amount_eur,
        line_type,
        is_cancellation
      FROM `${var.project_id}.${var.curated_dataset_id}.fct_sales_line`
    SQL
  }

  labels = var.labels
}

resource "google_bigquery_dataset_access" "authorized_sales_view" {
  project    = var.project_id
  dataset_id = var.curated_dataset_id

  view {
    project_id = var.project_id
    dataset_id = google_bigquery_dataset.semantic.dataset_id
    table_id   = google_bigquery_table.sales_view.table_id
  }
}

resource "google_bigquery_dataset_iam_member" "semantic_bi_viewer" {
  for_each = var.bi_reader_members

  project    = var.project_id
  dataset_id = google_bigquery_dataset.semantic.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = each.value
}

resource "google_project_iam_member" "bi_job_user" {
  for_each = var.bi_reader_members

  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = each.value
}

resource "google_dataplex_asset" "semantic_bigquery" {
  project       = var.project_id
  location      = var.region
  lake          = google_dataplex_lake.retail.name
  dataplex_zone = google_dataplex_zone.curated.name
  name          = "semantic-bigquery"
  display_name  = "Semantic BigQuery"
  labels        = var.labels

  discovery_spec {
    enabled = false
  }

  resource_spec {
    name = "projects/${var.project_id}/datasets/${google_bigquery_dataset.semantic.dataset_id}"
    type = "BIGQUERY_DATASET"
  }
}

# -----------------------------------------------------------------------------
# Dataplex service-agent permissions required by the on-demand DQ scan.
# Lake creation is used as a dependency so the Dataplex service identity exists.
# -----------------------------------------------------------------------------
resource "google_project_iam_member" "dataplex_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = local.dataplex_service_agent

  depends_on = [google_dataplex_lake.retail]
}

resource "google_bigquery_dataset_access" "dataplex_curated_reader" {
  project    = var.project_id
  dataset_id = var.curated_dataset_id
  role       = "READER"
  iam_member = local.dataplex_service_agent

  depends_on = [google_dataplex_lake.retail]
}

resource "google_bigquery_dataset_access" "dataplex_ops_reader" {
  project    = var.project_id
  dataset_id = var.ops_dataset_id
  role       = "READER"
  iam_member = local.dataplex_service_agent

  depends_on = [google_dataplex_lake.retail]
}

resource "google_kms_crypto_key_iam_member" "dataplex_cmek" {
  crypto_key_id = var.kms_key_id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = local.dataplex_service_agent

  depends_on = [google_dataplex_lake.retail]
}

# -----------------------------------------------------------------------------
# Independent C9 validation in Dataplex.
# The scan is on-demand so Module 12 can trigger it after a successful publish.
# C9-001/002/005 use the persisted batch-control record; C9-003/004 recompute
# directly against the published fact and dimensions.
# -----------------------------------------------------------------------------
resource "google_dataplex_datascan" "c9_quality" {
  project      = var.project_id
  location     = var.region
  data_scan_id = "retail-c9-quality"
  display_name = "Retail C9 independent data-quality scan"
  description  = "Independent Dataplex validation of the C9 publish gate."
  labels       = var.labels

  data {
    resource = local.fact_table_resource
  }

  execution_spec {
    trigger {
      on_demand {}
    }
  }

  data_quality_spec {
    sampling_percent           = 100
    catalog_publishing_enabled = false

    rules {
      name        = "c9-001-row-reconciliation"
      dimension   = "COMPLETENESS"
      description = "Published rows plus quarantined/excluded rows must reconcile to extracted rows."

      sql_assertion {
        sql_statement = <<-SQL
          WITH latest AS (
            SELECT *
            FROM ${local.batch_control_table}
            WHERE status = 'C9_PASSED'
            ORDER BY published_at DESC
            LIMIT 1
          )
          SELECT 'C9-001 reconciliation failed' AS error
          WHERE (SELECT COUNT(*) FROM latest) = 0
             OR EXISTS (
               SELECT 1
               FROM latest
               WHERE rows_extracted != published_rows + quarantined_rows_total + deliberately_excluded_rows
             )
        SQL
      }
    }

    rules {
      name        = "c9-002-control-total"
      dimension   = "ACCURACY"
      description = "GBP source/target control-total variance must be within +/-0.01."

      sql_assertion {
        sql_statement = <<-SQL
          WITH latest AS (
            SELECT *
            FROM ${local.batch_control_table}
            WHERE status = 'C9_PASSED'
            ORDER BY published_at DESC
            LIMIT 1
          )
          SELECT 'C9-002 control total failed' AS error
          WHERE (SELECT COUNT(*) FROM latest) = 0
             OR EXISTS (
               SELECT 1
               FROM latest
               WHERE ABS(source_control_total - target_control_total) > NUMERIC '0.01'
             )
        SQL
      }
    }

    rules {
      name        = "c9-003-no-duplicates"
      dimension   = "UNIQUENESS"
      description = "No duplicate invoice/stock_code business keys may exist in the published fact."

      sql_assertion {
        sql_statement = <<-SQL
          SELECT invoice, stock_code
          FROM $${data()}
          GROUP BY invoice, stock_code
          HAVING COUNT(*) > 1
        SQL
      }
    }

    rules {
      name        = "c9-004-no-orphans"
      dimension   = "INTEGRITY"
      description = "Dimension keys must resolve except reserved -1/-2 members."

      sql_assertion {
        sql_statement = <<-SQL
          SELECT f.product_key, f.customer_key
          FROM $${data()} AS f
          LEFT JOIN ${local.curated_product_table} AS p
            ON f.product_key = p.product_key
          LEFT JOIN ${local.curated_customer_table} AS c
            ON f.customer_key = c.customer_key
          WHERE (p.product_key IS NULL AND f.product_key NOT IN (-1, -2))
             OR (c.customer_key IS NULL AND f.customer_key NOT IN (-1, -2))
        SQL
      }
    }

    rules {
      name        = "c9-005-batch-date"
      dimension   = "FRESHNESS"
      description = "Newest published invoice date must equal the latest published business date."

      sql_assertion {
        sql_statement = <<-SQL
          WITH latest AS (
            SELECT business_date
            FROM ${local.batch_control_table}
            WHERE status = 'C9_PASSED'
            ORDER BY published_at DESC
            LIMIT 1
          ), fact_max AS (
            SELECT MAX(invoice_date_local) AS newest_invoice_date
            FROM $${data()}
          )
          SELECT 'C9-005 batch date failed' AS error
          WHERE (SELECT COUNT(*) FROM latest) = 0
             OR (SELECT newest_invoice_date FROM fact_max) != (SELECT business_date FROM latest)
        SQL
      }
    }
  }

  depends_on = [
    google_bigquery_dataset_access.dataplex_curated_reader,
    google_bigquery_dataset_access.dataplex_ops_reader,
    google_kms_crypto_key_iam_member.dataplex_cmek,
    google_dataplex_asset.curated_bigquery,
    google_dataplex_asset.ops_bigquery,
  ]
}
