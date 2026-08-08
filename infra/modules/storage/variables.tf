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