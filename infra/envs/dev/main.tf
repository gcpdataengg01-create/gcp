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

  labels                 = local.common_labels
  raw_lifecycle_age_days = var.raw_lifecycle_age_days

  depends_on = [
    google_project_service.required,
    module.security
  ]
}


module "compute" {
  source = "../../modules/compute"

  project_id  = var.project_id
  region      = var.region
  environment = var.environment
  labels      = local.common_labels

  subnet_name = module.networking.subnet_name

  kms_key_id = module.security.kms_key_id

  dataproc_service_account_email  = module.security.dataproc_service_account_email
  fx_service_account_email        = module.security.fx_service_account_email
  scheduler_service_account_email = module.security.scheduler_service_account_email

  raw_bucket_name = module.storage.raw_bucket_name

  cloudsql_private_ip    = module.data_store.private_ip_address
  cloudsql_database_name = module.data_store.database_name

  db_username_secret_id = module.security.db_secret_ids["postgres-username"]
  db_password_secret_id = module.security.db_secret_ids["postgres-password"]

  # Two-phase runtime bootstrap: first create Artifact Registry, then build/push
  # the FX image and apply again with deploy_fx_job=true + fx_image_uri set.
  deploy_fx_job = var.deploy_fx_job
  fx_image_uri  = var.fx_image_uri

  depends_on = [
    google_project_service.required,
    module.networking,
    module.security,
    module.data_store,
    module.storage
  ]
}
module "governance_policy" {
  source = "../../modules/governance_policy"

  project_id  = var.project_id
  region      = var.region
  environment = var.environment

  depends_on = [
    google_project_service.required
  ]
}

module "warehouse" {
  source = "../../modules/warehouse"

  project_id  = var.project_id
  region      = var.region
  environment = var.environment
  labels      = local.common_labels

  kms_key_id               = module.security.kms_key_id
  customer_policy_tag_name = module.governance_policy.customer_policy_tag_name

  bigquery_loader_service_account_email = module.security.bigquery_loader_service_account_email
  maximum_bytes_billed                  = var.bigquery_maximum_bytes_billed

  depends_on = [
    google_project_service.required,
    module.security,
    module.storage,
    module.governance_policy
  ]
}

module "governance" {
  source = "../../modules/governance"

  project_id  = var.project_id
  region      = var.region
  environment = var.environment
  labels      = local.common_labels

  raw_bucket_name        = module.storage.raw_bucket_name
  stage_bucket_name      = module.storage.stage_bucket_name
  curated_bucket_name    = module.storage.curated_bucket_name
  quarantine_bucket_name = module.storage.quarantine_bucket_name

  curated_dataset_id = module.warehouse.curated_dataset_id
  staging_dataset_id = module.warehouse.staging_dataset_id
  ops_dataset_id     = module.warehouse.ops_dataset_id

  kms_key_id        = module.security.kms_key_id
  bi_reader_members = var.bi_reader_members

  depends_on = [
    google_project_service.required,
    module.storage,
    module.warehouse
  ]
}

module "orchestration" {
  source = "../../modules/orchestration"

  project_id  = var.project_id
  region      = var.region
  environment = var.environment
  labels      = local.common_labels

  network_id = module.networking.network_id
  subnet_id  = module.networking.subnet_id
  kms_key_id = module.security.kms_key_id

  composer_service_account_email        = module.security.composer_service_account_email
  dataproc_service_account_email        = module.security.dataproc_service_account_email
  bigquery_loader_service_account_email = module.security.bigquery_loader_service_account_email

  dataproc_staging_bucket_name = module.compute.dataproc_staging_bucket_name
  raw_bucket_name              = module.storage.raw_bucket_name
  stage_bucket_name            = module.storage.stage_bucket_name
  curated_bucket_name          = module.storage.curated_bucket_name
  quarantine_bucket_name       = module.storage.quarantine_bucket_name

  cloudsql_private_ip    = module.data_store.private_ip_address
  cloudsql_database_name = module.data_store.database_name
  db_username_secret_id  = module.security.db_secret_ids["postgres-username"]
  db_password_secret_id  = module.security.db_secret_ids["postgres-password"]
  maximum_bytes_billed   = var.bigquery_maximum_bytes_billed

  composer_environment_size = var.composer_environment_size
  composer_image_version    = var.composer_image_version
  composer_pypi_packages    = var.composer_pypi_packages

  depends_on = [
    google_project_service.required,
    module.networking,
    module.security,
    module.compute,
    module.storage,
    module.warehouse,
    module.governance
  ]
}

module "observability" {
  source = "../../modules/observability"

  project_id            = var.project_id
  environment           = var.environment
  notification_channels = var.monitoring_notification_channels

  depends_on = [
    google_project_service.required,
    module.orchestration
  ]
}
