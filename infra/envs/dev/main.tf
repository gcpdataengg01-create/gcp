locals {
  common_labels = {
    owner       = "data-engineering"
    pipeline    = "retail-batch-etl"
    cost-centre = "data-001"
    environment = var.environment
    managed-by  = "terraform"
  }
}

module "networking" {
  source = "../../modules/networking"

  project_id  = var.project_id
  region      = var.region
  environment = var.environment

  subnet_cidr    = "10.10.0.0/24"
  connector_cidr = "10.10.10.0/28"

  depends_on = [
    google_project_service.required
  ]
}

module "security" {
  source = "../../modules/security"

  project_id  = var.project_id
  region      = var.region
  environment = var.environment
  labels      = local.common_labels

  depends_on = [
    google_project_service.required
  ]
}