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