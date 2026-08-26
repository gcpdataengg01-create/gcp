variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "Primary GCP region"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "kms_key_id" {
  description = "CMEK key used to encrypt GCS objects"
  type        = string
}

variable "dataproc_service_account_email" {
  type = string
}

variable "fx_service_account_email" {
  type = string
}

variable "bigquery_loader_service_account_email" {
  type = string
}

variable "labels" {
  type    = map(string)
  default = {}
}

variable "raw_lifecycle_age_days" {
  description = "Lifecycle age in days for immutable raw objects; configured at bucket creation for G-01"
  type        = number
  default     = 90

  validation {
    condition     = var.raw_lifecycle_age_days >= 30
    error_message = "raw_lifecycle_age_days must be at least 30 days."
  }
}
