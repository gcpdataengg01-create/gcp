variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "environment" {
  type = string
}

variable "network_id" {
  description = "VPC network ID used by Cloud SQL private IP"
  type        = string
}

variable "db_username_secret_id" {
  type = string
}

variable "db_password_secret_id" {
  type = string
}

variable "db_database_secret_id" {
  type = string
}