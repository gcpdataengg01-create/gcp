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