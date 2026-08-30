data "google_project" "current" {
  project_id = var.project_id
}

locals {
  curated_dataset_id = "curated"
  staging_dataset_id = "staging"
  ops_dataset_id     = "ops"
}

# BigQuery's CMEK service account must be able to use the data key.
resource "google_kms_crypto_key_iam_member" "bigquery_cmek" {
  crypto_key_id = var.kms_key_id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:bq-${data.google_project.current.number}@bigquery-encryption.iam.gserviceaccount.com"
}

resource "google_bigquery_dataset" "curated" {
  project    = var.project_id
  dataset_id = local.curated_dataset_id
  location   = var.region

  delete_contents_on_destroy = var.environment == "dev"

  default_encryption_configuration {
    kms_key_name = var.kms_key_id
  }

  labels = var.labels

  lifecycle {
    ignore_changes = [access]
  }

  depends_on = [google_kms_crypto_key_iam_member.bigquery_cmek]
}

resource "google_bigquery_dataset" "staging" {
  project    = var.project_id
  dataset_id = local.staging_dataset_id
  location   = var.region

  delete_contents_on_destroy = var.environment == "dev"

  default_partition_expiration_ms = 14 * 24 * 60 * 60 * 1000
  default_table_expiration_ms     = 14 * 24 * 60 * 60 * 1000

  default_encryption_configuration {
    kms_key_name = var.kms_key_id
  }

  labels = var.labels

  lifecycle {
    ignore_changes = [access]
  }

  depends_on = [google_kms_crypto_key_iam_member.bigquery_cmek]
}

resource "google_bigquery_dataset" "ops" {
  project    = var.project_id
  dataset_id = local.ops_dataset_id
  location   = var.region

  delete_contents_on_destroy = var.environment == "dev"

  default_table_expiration_ms = 90 * 24 * 60 * 60 * 1000

  default_encryption_configuration {
    kms_key_name = var.kms_key_id
  }

  labels = var.labels

  lifecycle {
    ignore_changes = [access]
  }

  depends_on = [google_kms_crypto_key_iam_member.bigquery_cmek]
}

resource "google_bigquery_table" "dim_date" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.curated.dataset_id
  table_id   = "dim_date"

  deletion_protection = var.environment != "dev"
  schema              = file("${path.module}/schemas/dim_date.json")
  labels              = var.labels

  encryption_configuration {
    kms_key_name = var.kms_key_id
  }
}

resource "google_bigquery_table" "dim_customer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.curated.dataset_id
  table_id   = "dim_customer"

  deletion_protection = var.environment != "dev"
  schema = templatefile("${path.module}/schemas/dim_customer.json.tftpl", {
    customer_policy_tag_name = var.customer_policy_tag_name
  })
  labels = var.labels
  encryption_configuration {
    kms_key_name = var.kms_key_id
  }

}

resource "google_bigquery_table" "dim_product" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.curated.dataset_id
  table_id   = "dim_product"

  deletion_protection = var.environment != "dev"
  schema              = file("${path.module}/schemas/dim_product.json")
  labels              = var.labels

  encryption_configuration {
    kms_key_name = var.kms_key_id
  }
}

resource "google_bigquery_table" "fct_sales_line" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.curated.dataset_id
  table_id   = "fct_sales_line"

  deletion_protection = var.environment != "dev"

  time_partitioning {
    type  = "DAY"
    field = "invoice_date_local"
  }

  clustering = ["product_key", "country_code"]
  schema = templatefile("${path.module}/schemas/fct_sales_line.json.tftpl", {
    customer_policy_tag_name = var.customer_policy_tag_name
  })
  labels = var.labels

  encryption_configuration {
    kms_key_name = var.kms_key_id
  }
}

resource "google_bigquery_table" "fct_sales_line_stg" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.staging.dataset_id
  table_id   = "fct_sales_line_stg"

  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "invoice_date_local"
  }

  clustering = ["product_key", "country_code"]
  schema = templatefile("${path.module}/schemas/fct_sales_line.json.tftpl", {
    customer_policy_tag_name = var.customer_policy_tag_name
  })
  labels = var.labels

  encryption_configuration {
    kms_key_name = var.kms_key_id
  }
}

resource "google_bigquery_table" "etl_batch_control" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.ops.dataset_id
  table_id   = "etl_batch_control"

  deletion_protection = var.environment != "dev"

  time_partitioning {
    type  = "DAY"
    field = "business_date"
  }

  clustering = ["status", "run_id"]
  schema     = file("${path.module}/schemas/etl_batch_control.json")
  labels     = var.labels
  encryption_configuration {
    kms_key_name = var.kms_key_id
  }
}

# The loader creates run-scoped staging tables at runtime. Only the staging
# dataset is infrastructure; its 14-day defaults ensure abandoned tables expire.
resource "google_project_iam_member" "loader_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.bigquery_loader_service_account_email}"
}

resource "google_project_iam_member" "loader_firestore_user" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${var.bigquery_loader_service_account_email}"
}

resource "google_bigquery_dataset_access" "loader_curated_writer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.curated.dataset_id
  role       = "WRITER"
  iam_member = "serviceAccount:${var.bigquery_loader_service_account_email}"
}

resource "google_bigquery_dataset_access" "loader_staging_writer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.staging.dataset_id
  role       = "WRITER"
  iam_member = "serviceAccount:${var.bigquery_loader_service_account_email}"
}

resource "google_bigquery_dataset_access" "loader_ops_writer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.ops.dataset_id
  role       = "WRITER"
  iam_member = "serviceAccount:${var.bigquery_loader_service_account_email}"
}



# G-20: attributable BigQuery query-cost view. INFORMATION_SCHEMA job views
# require a region qualifier that matches the dataset/query location.
resource "google_bigquery_table" "batch_etl_query_usage" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.ops.dataset_id
  table_id   = "v_batch_etl_query_usage"

  deletion_protection = var.environment != "dev"
  labels              = var.labels

  view {
    use_legacy_sql = false
    query          = <<-SQL
      SELECT
        creation_time,
        job_id,
        user_email,
        query,
        total_bytes_processed,
        total_bytes_billed,
        SAFE_DIVIDE(total_bytes_billed, POW(1024, 4)) AS billed_tib,
        total_slot_ms,
        labels
      FROM `${var.project_id}.region-${var.region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
      WHERE job_type = 'QUERY'
        AND state = 'DONE'
        AND EXISTS (
          SELECT 1
          FROM UNNEST(labels) AS label
          WHERE label.key = 'pipeline'
            AND label.value = 'batch-etl-retail'
        )
    SQL
  }
}
