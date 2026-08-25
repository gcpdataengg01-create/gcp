//Avoid hardcoding values, reuse the same code across different projects.

variable "project_id" {
  description = "Google Cloud Project ID"
  type        = string
}

variable "region" {
  description = "Google Cloud Region"
  type        = string
}

variable "zone" {
  description = "Google Cloud Zone"
  type        = string
}

variable "environment" {
  description = "Environment"
  type        = string
  default     = "dev"
}
variable "bigquery_maximum_bytes_billed" {
  description = "Per-query BigQuery maximum_bytes_billed guardrail for the retail ETL"
  type        = number
  default     = 10737418240
}
