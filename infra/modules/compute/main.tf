locals {
  name_prefix = "retail-etl-${var.environment}"

  dataproc_staging_bucket = "${var.project_id}-retail-etl-dataproc-${var.environment}"
  artifact_repo_name      = "retail-etl-${var.environment}"
  fx_job_name             = "retail-etl-${var.environment}-fx"
  fx_scheduler_name       = "retail-etl-${var.environment}-fx-schedule"
}

# -------------------------------------------------------------------
# Dataproc Serverless staging bucket
# -------------------------------------------------------------------

resource "google_storage_bucket" "dataproc_staging" {
  name     = local.dataproc_staging_bucket
  project  = var.project_id
  location = var.region

  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  force_destroy = var.environment == "dev"

  encryption {
    default_kms_key_name = var.kms_key_id
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }

    condition {
      age = 14
    }
  }

  labels = merge(
    var.labels,
    {
      component = "dataproc"
      purpose   = "staging"
    }
  )
}

# Dataproc SA needs to upload Spark code / staging artifacts.
resource "google_storage_bucket_iam_member" "dataproc_staging_admin" {
  bucket = google_storage_bucket.dataproc_staging.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.dataproc_service_account_email}"
}

# -------------------------------------------------------------------
# Artifact Registry for the FX Cloud Run Job container image
# -------------------------------------------------------------------

resource "google_artifact_registry_repository" "fx" {
  project       = var.project_id
  location      = var.region
  repository_id = local.artifact_repo_name

  description = "Docker repository for Retail ETL runtime images"
  format      = "DOCKER"

  labels = merge(
    var.labels,
    {
      component = "artifact-registry"
      workload  = "fx"
    }
  )

  docker_config {
    immutable_tags = false
  }
}

# Allow the FX runtime service account to pull images.
resource "google_artifact_registry_repository_iam_member" "fx_reader" {
  project    = var.project_id
  location   = google_artifact_registry_repository.fx.location
  repository = google_artifact_registry_repository.fx.name

  role   = "roles/artifactregistry.reader"
  member = "serviceAccount:${var.fx_service_account_email}"
}

# -------------------------------------------------------------------
# Raw bucket permission for FX reference data is owned by the storage module.
# Keeping a single Terraform owner avoids duplicate IAM member management.
# -------------------------------------------------------------------

# -------------------------------------------------------------------
# Cloud Run FX Job
#
# deploy_fx_job=false initially because Artifact Registry must exist
# before the Docker image can be pushed.
# -------------------------------------------------------------------

resource "google_cloud_run_v2_job" "fx" {
  count = var.deploy_fx_job ? 1 : 0

  project  = var.project_id
  location = var.region
  name     = local.fx_job_name

  labels = merge(
    var.labels,
    {
      component = "cloud-run"
      workload  = "fx"
    }
  )

  template {
    task_count = 1

    template {
      service_account = var.fx_service_account_email

      max_retries = 2
      timeout     = "600s"

      containers {
        image = var.fx_image_uri

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }

        env {
          name  = "PROJECT_ID"
          value = var.project_id
        }

        env {
          name  = "RAW_BUCKET"
          value = var.raw_bucket_name
        }

        env {
          name  = "FX_BASE"
          value = "GBP"
        }

        env {
          name  = "FX_QUOTE"
          value = "EUR"
        }
      }
    }
  }
}

# -------------------------------------------------------------------
# Scheduler -> Cloud Run Job invocation
#
# Google documents the Cloud Run v2 jobs :run endpoint for Scheduler.
# -------------------------------------------------------------------

resource "google_cloud_run_v2_job_iam_member" "scheduler_invoker" {
  count = var.deploy_fx_job ? 1 : 0

  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.fx[0].name

  role   = "roles/run.invoker"
  member = "serviceAccount:${var.scheduler_service_account_email}"
}

resource "google_cloud_scheduler_job" "fx" {
  count = var.deploy_fx_job ? 1 : 0

  project = var.project_id
  region  = var.region

  name        = local.fx_scheduler_name
  description = "Runs the Retail ETL GBP to EUR FX extraction job"

  schedule  = var.fx_schedule
  time_zone = var.fx_schedule_time_zone

  attempt_deadline = "600s"

  retry_config {
    retry_count          = 3
    min_backoff_duration = "30s"
    max_backoff_duration = "300s"
    max_doublings        = 3
  }

  http_target {
    http_method = "POST"

    uri = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${google_cloud_run_v2_job.fx[0].name}:run"

    headers = {
      "Content-Type" = "application/json"
    }

    body = base64encode("{}")

    oauth_token {
      service_account_email = var.scheduler_service_account_email
    }
  }

  depends_on = [
    google_cloud_run_v2_job_iam_member.scheduler_invoker
  ]
}