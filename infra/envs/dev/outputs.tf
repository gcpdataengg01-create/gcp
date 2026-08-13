output "network_name" {
  value = module.networking.network_name
}

output "subnet_name" {
  value = module.networking.subnet_name
}

output "serverless_connector_id" {
  value = module.networking.serverless_connector_id
}


output "kms_key_id" {
  value = module.security.kms_key_id
}

output "kms_key_ring_id" {
  value = module.security.kms_key_ring_id
}

output "dataproc_service_account_email" {
  value = module.security.dataproc_service_account_email
}

output "composer_service_account_email" {
  value = module.security.composer_service_account_email
}

output "fx_service_account_email" {
  value = module.security.fx_service_account_email
}

output "scheduler_service_account_email" {
  value = module.security.scheduler_service_account_email
}

output "bigquery_loader_service_account_email" {
  value = module.security.bigquery_loader_service_account_email
}

output "db_secret_ids" {
  value = module.security.db_secret_ids
}

output "cloudsql_instance_name" {
  value = module.data_store.instance_name
}

output "cloudsql_private_ip" {
  value = module.data_store.private_ip_address
}

output "cloudsql_database_name" {
  value = module.data_store.database_name
}

output "cloudsql_database_user" {
  value = module.data_store.database_user
}

output "storage_bucket_names" {
  value = module.storage.bucket_names
}

output "raw_bucket_name" {
  value = module.storage.raw_bucket_name
}

output "stage_bucket_name" {
  value = module.storage.stage_bucket_name
}

output "curated_bucket_name" {
  value = module.storage.curated_bucket_name
}

output "quarantine_bucket_name" {
  value = module.storage.quarantine_bucket_name
}

output "firestore_database_name" {
  value = module.data_store.firestore_database_name
}

output "dataproc_staging_bucket_name" {
  value = module.compute.dataproc_staging_bucket_name
}

output "artifact_registry_repository_name" {
  value = module.compute.artifact_registry_repository_name
}

output "fx_image_repository" {
  value = module.compute.fx_image_repository
}

output "fx_job_name" {
  value = module.compute.fx_job_name
}

output "fx_scheduler_name" {
  value = module.compute.fx_scheduler_name
}