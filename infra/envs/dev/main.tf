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

module "data_store" {
  source = "../../modules/data_store"

  project_id  = var.project_id
  region      = var.region
  environment = var.environment

  network_id = module.networking.network_id

  db_username_secret_id = module.security.db_secret_ids["postgres-username"]
  db_password_secret_id = module.security.db_secret_ids["postgres-password"]
  db_database_secret_id = module.security.db_secret_ids["postgres-database"]

  dataproc_service_account_email = module.security.dataproc_service_account_email

  depends_on = [
    google_project_service.required,
    module.networking,
    module.security
  ]
}

module "storage" {
  source = "../../modules/storage"

  project_id  = var.project_id
  region      = var.region
  environment = var.environment

  kms_key_id = module.security.kms_key_id

  dataproc_service_account_email        = module.security.dataproc_service_account_email
  fx_service_account_email              = module.security.fx_service_account_email
  bigquery_loader_service_account_email = module.security.bigquery_loader_service_account_email

  labels = local.common_labels

  depends_on = [
    google_project_service.required,
    module.security
  ]
}