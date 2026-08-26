locals {
  composer_name = "retail-etl-${var.environment}-composer"
  code_bucket   = "${var.project_id}-retail-etl-code-${var.environment}"
  fx_job_name   = "retail-etl-${var.environment}-fx"
}

resource "google_storage_bucket" "code" {
  project  = var.project_id
  name     = local.code_bucket
  location = var.region

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = var.environment == "dev"

  versioning { enabled = true }

  encryption {
    default_kms_key_name = var.kms_key_id
  }

  labels = merge(var.labels, {
    component = "orchestration"
    purpose   = "spark-code"
  })
}

resource "google_storage_bucket_iam_member" "dataproc_code_reader" {
  bucket = google_storage_bucket.code.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${var.dataproc_service_account_email}"
}

resource "google_storage_bucket_iam_member" "bq_loader_code_reader" {
  bucket = google_storage_bucket.code.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${var.bigquery_loader_service_account_email}"
}

resource "google_composer_environment" "retail" {
  project = var.project_id
  name    = local.composer_name
  region  = var.region
  labels  = var.labels

  config {
    environment_size = var.composer_environment_size

    node_config {
      network         = var.network_id
      subnetwork      = var.subnet_id
      service_account = var.composer_service_account_email
    }

    software_config {
      image_version = var.composer_image_version
      pypi_packages = var.composer_pypi_packages
      env_variables = {
        RETAIL_PROJECT_ID                = var.project_id
        RETAIL_REGION                    = var.region
        RETAIL_ENVIRONMENT               = var.environment
        RETAIL_CODE_BUCKET               = google_storage_bucket.code.name
        RETAIL_DATAPROC_STAGING_BUCKET   = var.dataproc_staging_bucket_name
        RETAIL_DATAPROC_SERVICE_ACCOUNT  = var.dataproc_service_account_email
        RETAIL_BQ_LOADER_SERVICE_ACCOUNT = var.bigquery_loader_service_account_email
        RETAIL_SUBNETWORK_URI            = var.subnet_id
        RETAIL_RAW_BUCKET                = var.raw_bucket_name
        RETAIL_STAGE_BUCKET              = var.stage_bucket_name
        RETAIL_CURATED_BUCKET            = var.curated_bucket_name
        RETAIL_QUARANTINE_BUCKET         = var.quarantine_bucket_name
        RETAIL_DB_HOST                   = var.cloudsql_private_ip
        RETAIL_DB_NAME                   = var.cloudsql_database_name
        RETAIL_DB_USER_SECRET            = var.db_username_secret_id
        RETAIL_DB_PASSWORD_SECRET        = var.db_password_secret_id
        RETAIL_FX_JOB_NAME               = local.fx_job_name
        RETAIL_DATAPLEX_SCAN_ID          = "retail-c9-quality"
        RETAIL_MAXIMUM_BYTES_BILLED      = tostring(var.maximum_bytes_billed)
      }
    }
  }
}

resource "google_storage_bucket_iam_member" "bq_loader_dataproc_staging" {
  bucket = var.dataproc_staging_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.bigquery_loader_service_account_email}"
}
