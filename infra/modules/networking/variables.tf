variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "environment" {
  type = string
}

variable "subnet_cidr" {
  type    = string
  default = "10.10.0.0/24"
}

variable "connector_cidr" {
  type    = string
  default = "10.10.10.0/28"
}