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

variable "bi_reader_members" {
  description = "Optional BI principals allowed to query semantic authorized views"
  type        = set(string)
  default     = []
}

variable "composer_environment_size" {
  description = "Cloud Composer 3 environment size"
  type        = string
  default     = "ENVIRONMENT_SIZE_SMALL"
}

variable "composer_image_version" {
  description = "Optional fully pinned Composer image version; null uses the service default"
  type        = string
  default     = null
}

variable "monitoring_notification_channels" {
  description = "Optional existing Cloud Monitoring notification channel IDs"
  type        = list(string)
  default     = []
}

variable "deploy_fx_job" {
  description = "Create the FX Cloud Run Job after its container image has been pushed"
  type        = bool
  default     = false
}

variable "fx_image_uri" {
  description = "Artifact Registry image URI for the FX Cloud Run Job"
  type        = string
  default     = ""
}
