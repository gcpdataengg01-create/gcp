output "composer_environment_name" {
  value = google_composer_environment.retail.name
}

output "composer_dag_gcs_prefix" {
  value = google_composer_environment.retail.config[0].dag_gcs_prefix
}

output "code_bucket_name" {
  value = google_storage_bucket.code.name
}
