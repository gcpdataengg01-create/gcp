data "google_project" "current" {
  project_id = var.project_id
}

locals {
  curated_dataset_id = "curated"
  staging_dataset_id = "staging"
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

  depends_on = [google_kms_crypto_key_iam_member.bigquery_cmek]
}

resource "google_bigquery_table" "dim_date" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.curated.dataset_id
  table_id   = "dim_date"

  deletion_protection = var.environment != "dev"
  schema              = file("${path.module}/schemas/dim_date.json")
  labels              = var.labels
}

resource "google_bigquery_table" "dim_customer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.curated.dataset_id
  table_id   = "dim_customer"

  deletion_protection = var.environment != "dev"
  schema              = file("${path.module}/schemas/dim_customer.json")
  labels              = var.labels
}

resource "google_bigquery_table" "dim_product" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.curated.dataset_id
  table_id   = "dim_product"

  deletion_protection = var.environment != "dev"
  schema              = file("${path.module}/schemas/dim_product.json")
  labels              = var.labels
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
  schema     = file("${path.module}/schemas/fct_sales_line.json")
  labels     = var.labels
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

resource "google_bigquery_dataset_iam_member" "loader_curated_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.curated.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.bigquery_loader_service_account_email}"
}

resource "google_bigquery_dataset_iam_member" "loader_staging_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.staging.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.bigquery_loader_service_account_email}"
}
