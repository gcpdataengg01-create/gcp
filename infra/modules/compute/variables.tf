variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "labels" {
  description = "Common labels"
  type        = map(string)
  default     = {}
}

variable "subnet_name" {
  description = "Subnet used by Dataproc Serverless"
  type        = string
}

variable "kms_key_id" {
  description = "CMEK key used by Module 7 resources"
  type        = string
}

variable "dataproc_service_account_email" {
  description = "Dataproc Serverless workload service account"
  type        = string
}

variable "fx_service_account_email" {
  description = "Cloud Run FX job service account"
  type        = string
}

variable "scheduler_service_account_email" {
  description = "Cloud Scheduler service account"
  type        = string
}

variable "raw_bucket_name" {
  description = "Existing raw GCS bucket"
  type        = string
}

variable "cloudsql_private_ip" {
  description = "Cloud SQL private IP for JDBC extraction"
  type        = string
}

variable "cloudsql_database_name" {
  description = "Cloud SQL database name"
  type        = string
}

variable "db_username_secret_id" {
  description = "Secret Manager resource ID for DB username"
  type        = string
}

variable "db_password_secret_id" {
  description = "Secret Manager resource ID for DB password"
  type        = string
}

variable "fx_schedule" {
  description = "Cron schedule for FX extraction"
  type        = string
  default     = "0 2 * * *"
}

variable "fx_schedule_time_zone" {
  description = "Cloud Scheduler timezone"
  type        = string
  default     = "Etc/UTC"
}

variable "deploy_fx_job" {
  description = "Create Cloud Run FX job and Scheduler after image exists"
  type        = bool
  default     = false
}

variable "fx_image_uri" {
  description = "Container image URI for FX Cloud Run job"
  type        = string
  default     = ""
}