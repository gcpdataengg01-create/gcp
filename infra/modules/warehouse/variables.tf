variable "project_id" {
  description = "Google Cloud project ID"
  type        = string
}

variable "region" {
  description = "BigQuery dataset location"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "kms_key_id" {
  description = "CMEK key used by curated/staging BigQuery datasets"
  type        = string
}

variable "bigquery_loader_service_account_email" {
  description = "Service account used by the warehouse load/publish component"
  type        = string
}

variable "labels" {
  description = "Common resource labels"
  type        = map(string)
  default     = {}
}

variable "maximum_bytes_billed" {
  description = "Per-query BigQuery guardrail supplied to application/Composer jobs"
  type        = number
  default     = 10737418240
}
