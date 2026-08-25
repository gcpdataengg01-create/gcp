variable "project_id" {
  description = "Google Cloud project ID"
  type        = string
}

variable "region" {
  description = "Dataplex and BigQuery location"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "labels" {
  description = "Common resource labels"
  type        = map(string)
  default     = {}
}

variable "raw_bucket_name" {
  description = "Raw GCS bucket"
  type        = string
}

variable "stage_bucket_name" {
  description = "Stage GCS bucket"
  type        = string
}

variable "curated_bucket_name" {
  description = "Curated GCS bucket"
  type        = string
}

variable "quarantine_bucket_name" {
  description = "Quarantine GCS bucket"
  type        = string
}

variable "curated_dataset_id" {
  description = "Curated BigQuery dataset"
  type        = string
}

variable "staging_dataset_id" {
  description = "Staging BigQuery dataset"
  type        = string
}

variable "ops_dataset_id" {
  description = "Operational-control BigQuery dataset"
  type        = string
}

variable "kms_key_id" {
  description = "CMEK key used by governed BigQuery datasets"
  type        = string
}

variable "bi_reader_members" {
  description = "Optional BI principals allowed to query the semantic dataset, e.g. group:bi@example.com"
  type        = set(string)
  default     = []
}
