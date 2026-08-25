variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "environment" {
  type = string
}

variable "labels" {
  type    = map(string)
  default = {}
}

variable "network_id" { type = string }
variable "subnet_id" { type = string }
variable "kms_key_id" { type = string }
variable "composer_service_account_email" { type = string }
variable "dataproc_service_account_email" { type = string }
variable "bigquery_loader_service_account_email" { type = string }
variable "dataproc_staging_bucket_name" { type = string }
variable "raw_bucket_name" { type = string }
variable "stage_bucket_name" { type = string }
variable "curated_bucket_name" { type = string }
variable "quarantine_bucket_name" { type = string }
variable "cloudsql_private_ip" { type = string }
variable "cloudsql_database_name" { type = string }
variable "db_username_secret_id" { type = string }
variable "db_password_secret_id" { type = string }
variable "maximum_bytes_billed" { type = number }

variable "composer_environment_size" {
  type    = string
  default = "ENVIRONMENT_SIZE_SMALL"
}

variable "composer_image_version" {
  description = "Optional fully pinned Composer image version. Null uses the service default."
  type        = string
  default     = null
}
