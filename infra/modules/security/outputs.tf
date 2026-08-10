output "kms_key_id" {
  description = "CMEK crypto key ID"
  value       = google_kms_crypto_key.data.id
}

output "kms_key_ring_id" {
  description = "KMS key ring ID"
  value       = google_kms_key_ring.etl.id
}

output "dataproc_service_account_email" {
  description = "Dataproc service account email"
  value       = google_service_account.etl["dataproc"].email
}

output "composer_service_account_email" {
  description = "Composer service account email"
  value       = google_service_account.etl["composer"].email
}

output "fx_service_account_email" {
  description = "Cloud Run FX service account email"
  value       = google_service_account.etl["fx"].email
}

output "scheduler_service_account_email" {
  description = "Cloud Scheduler service account email"
  value       = google_service_account.etl["scheduler"].email
}

output "bigquery_loader_service_account_email" {
  description = "BigQuery loader service account email"
  value       = google_service_account.etl["bq_loader"].email
}

output "db_secret_ids" {
  description = "Database Secret Manager secret IDs"

  value = {
    for key, secret in google_secret_manager_secret.db :
    key => secret.id
  }
}