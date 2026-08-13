output "dataproc_staging_bucket_name" {
  description = "Dataproc Serverless staging bucket"
  value       = google_storage_bucket.dataproc_staging.name
}

output "artifact_registry_repository_name" {
  description = "Artifact Registry Docker repository name"
  value       = google_artifact_registry_repository.fx.name
}

output "artifact_registry_repository_id" {
  description = "Artifact Registry repository ID"
  value       = google_artifact_registry_repository.fx.id
}

output "fx_image_repository" {
  description = "Base Docker repository URI for FX image"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.fx.repository_id}"
}

output "fx_job_name" {
  description = "Cloud Run FX job name"
  value       = var.deploy_fx_job ? google_cloud_run_v2_job.fx[0].name : null
}

output "fx_scheduler_name" {
  description = "FX Scheduler job name"
  value       = var.deploy_fx_job ? google_cloud_scheduler_job.fx[0].name : null
}