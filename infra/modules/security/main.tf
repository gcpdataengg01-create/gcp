data "google_project" "current" {
  project_id = var.project_id
}

# =========================================================
# KMS
# =========================================================

resource "google_kms_key_ring" "etl" {
  project  = var.project_id
  name     = "retail-etl-${var.environment}-keyring"
  location = var.region
}

resource "google_kms_crypto_key" "data" {
  name     = "retail-etl-${var.environment}-data-key"
  key_ring = google_kms_key_ring.etl.id

  rotation_period = "7776000s"

  labels = var.labels

  lifecycle {
    prevent_destroy = true
  }
}


# =========================================================
# SERVICE ACCOUNTS
# =========================================================

locals {
  service_accounts = {
    dataproc = {
      account_id   = "sa-dataproc"
      display_name = "Retail ETL Dataproc Service Account"
    }

    fx = {
      account_id   = "sa-cloud-run-fx"
      display_name = "Retail ETL FX Cloud Run Service Account"
    }

    composer = {
      account_id   = "sa-composer"
      display_name = "Retail ETL Composer Service Account"
    }

    scheduler = {
      account_id   = "sa-scheduler"
      display_name = "Retail ETL Scheduler Service Account"
    }

    bq_loader = {
      account_id   = "sa-bigquery-loader"
      display_name = "Retail ETL BigQuery Loader Service Account"
    }
  }
}

resource "google_service_account" "etl" {
  for_each = local.service_accounts

  project      = var.project_id
  account_id   = each.value.account_id
  display_name = each.value.display_name
}


# =========================================================
# DATAPROC IAM
# Target runtime later: Serverless Spark 2.3 LTS
# =========================================================

resource "google_project_iam_member" "dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"

  member = "serviceAccount:${google_service_account.etl["dataproc"].email}"
}

resource "google_project_iam_member" "dataproc_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"

  member = "serviceAccount:${google_service_account.etl["dataproc"].email}"
}

resource "google_project_iam_member" "dataproc_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"

  member = "serviceAccount:${google_service_account.etl["dataproc"].email}"
}


# =========================================================
# COMPOSER IAM
# =========================================================

resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"

  member = "serviceAccount:${google_service_account.etl["composer"].email}"
}

resource "google_project_iam_member" "composer_dataproc_editor" {
  project = var.project_id
  role    = "roles/dataproc.editor"

  member = "serviceAccount:${google_service_account.etl["composer"].email}"
}

resource "google_service_account_iam_member" "composer_use_dataproc_sa" {
  service_account_id = google_service_account.etl["dataproc"].name

  role = "roles/iam.serviceAccountUser"

  member = "serviceAccount:${google_service_account.etl["composer"].email}"
}


# =========================================================
# SECRET MANAGER
# Create containers now.
# Secret values/versions will be added with Cloud SQL.
# =========================================================

locals {
  db_secret_names = toset([
    "postgres-username",
    "postgres-password",
    "postgres-database"
  ])
}

resource "google_secret_manager_secret" "db" {
  for_each = local.db_secret_names

  project   = var.project_id
  secret_id = "retail-etl-${var.environment}-${each.value}"

  replication {
    auto {}
  }

  labels = var.labels
}


# =========================================================
# DATAPROC -> SECRET MANAGER ACCESS
# Access only these specific database secrets.
# =========================================================

resource "google_secret_manager_secret_iam_member" "dataproc_db_access" {
  for_each = google_secret_manager_secret.db

  project   = var.project_id
  secret_id = each.value.secret_id

  role   = "roles/secretmanager.secretAccessor"
  member = "serviceAccount:${google_service_account.etl["dataproc"].email}"
}
# =========================================================
# MODULE 12 ORCHESTRATION / RUNTIME IAM
# =========================================================

resource "google_project_iam_member" "composer_firestore_user" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${google_service_account.etl["composer"].email}"
}

resource "google_project_iam_member" "composer_dataplex_scan_editor" {
  project = var.project_id
  role    = "roles/dataplex.dataScanEditor"
  member  = "serviceAccount:${google_service_account.etl["composer"].email}"
}

resource "google_project_iam_member" "composer_network_user" {
  project = var.project_id
  role    = "roles/compute.networkUser"
  member  = "serviceAccount:${google_service_account.etl["composer"].email}"
}

resource "google_project_iam_member" "dataproc_network_user" {
  project = var.project_id
  role    = "roles/compute.networkUser"
  member  = "serviceAccount:${google_service_account.etl["dataproc"].email}"
}

# The BigQuery loader runs its stage/publish/commit actions as small Dataproc
# Serverless driver batches so the Composer DAG can keep those stages separate.
resource "google_project_iam_member" "bq_loader_dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.etl["bq_loader"].email}"
}

resource "google_project_iam_member" "bq_loader_network_user" {
  project = var.project_id
  role    = "roles/compute.networkUser"
  member  = "serviceAccount:${google_service_account.etl["bq_loader"].email}"
}

resource "google_project_iam_member" "bq_loader_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.etl["bq_loader"].email}"
}

resource "google_service_account_iam_member" "composer_use_bq_loader_sa" {
  service_account_id = google_service_account.etl["bq_loader"].name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.etl["composer"].email}"
}

resource "google_project_iam_member" "composer_cloud_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.etl["composer"].email}"
}

resource "google_service_account_iam_member" "composer_service_agent_extension" {
  service_account_id = google_service_account.etl["composer"].name
  role               = "roles/composer.ServiceAgentV2Ext"
  member             = "serviceAccount:service-${data.google_project.current.number}@cloudcomposer-accounts.iam.gserviceaccount.com"
}
