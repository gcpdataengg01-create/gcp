# =========================================================
# CLOUD STORAGE SERVICE AGENT
# Required so GCS can use our CMEK.
# =========================================================

data "google_storage_project_service_account" "gcs" {
  project = var.project_id
}


# =========================================================
# KMS ACCESS FOR CLOUD STORAGE
# =========================================================

resource "google_kms_crypto_key_iam_member" "gcs_cmek" {
  crypto_key_id = var.kms_key_id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = data.google_storage_project_service_account.gcs.member
}


# =========================================================
# DATA LAKE ZONES
# raw / stage / curated / quarantine
# =========================================================

locals {
  zones = toset([
    "raw",
    "stage",
    "curated",
    "quarantine"
  ])
}

resource "google_storage_bucket" "zone" {
  for_each = local.zones

  project  = var.project_id
  name     = "${var.project_id}-retail-etl-${each.key}-${var.environment}"
  location = var.region

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  force_destroy = var.environment == "dev"

  # Raw objects are versioned so accidental object replacement/deletion remains
  # recoverable. Raw write paths are also unique by run_id and written with
  # error-if-exists semantics in Spark.
  versioning {
    enabled = each.key == "raw"
  }

  autoclass {
    enabled = true
  }

  encryption {
    default_kms_key_name = var.kms_key_id
  }

  # G-01 requires raw lifecycle/retention to be configured at bucket creation.
  # Stage remains short-lived; raw is retained longer for replay/audit evidence.
  # A lifecycle policy is used instead of a bucket retention lock because Spark
  # committers may create and clean temporary objects during a write.
  dynamic "lifecycle_rule" {
    for_each = each.key == "stage" ? [14] : (each.key == "raw" ? [var.raw_lifecycle_age_days] : [])

    content {
      condition {
        age = lifecycle_rule.value
      }

      action {
        type = "Delete"
      }
    }
  }

  labels = var.labels

  depends_on = [
    google_kms_crypto_key_iam_member.gcs_cmek
  ]
}


# =========================================================
# DATAPROC ACCESS
# Dataproc reads/writes ETL objects in all four zones.
# =========================================================

resource "google_storage_bucket_iam_member" "dataproc_object_admin" {
  for_each = {
    for zone_name, bucket in google_storage_bucket.zone :
    zone_name => bucket if zone_name != "raw"
  }

  bucket = each.value.name
  role   = "roles/storage.objectAdmin"

  member = "serviceAccount:${var.dataproc_service_account_email}"
}

# Raw uses the narrower objectUser role. It still supports Spark's temporary
# object commit/rename behavior, while avoiding object-ACL administration.
resource "google_storage_bucket_iam_member" "dataproc_raw_object_user" {
  bucket = google_storage_bucket.zone["raw"].name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${var.dataproc_service_account_email}"
}


# =========================================================
# CLOUD RUN FX ACCESS
# FX job only needs to write into RAW.
# =========================================================

resource "google_storage_bucket_iam_member" "fx_raw_writer" {
  bucket = google_storage_bucket.zone["raw"].name
  role   = "roles/storage.objectCreator"

  member = "serviceAccount:${var.fx_service_account_email}"
}


# =========================================================
# BIGQUERY LOADER ACCESS
# BigQuery loading component reads curated Parquet files.
# =========================================================

resource "google_storage_bucket_iam_member" "bq_curated_reader" {
  bucket = google_storage_bucket.zone["curated"].name
  role   = "roles/storage.objectViewer"

  member = "serviceAccount:${var.bigquery_loader_service_account_email}"
}