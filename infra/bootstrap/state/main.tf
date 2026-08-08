resource "google_storage_bucket" "terraform_state" {
  project  = var.project_id
  name     = "${var.project_id}-retail-etl-tfstate"
  location = var.region

  uniform_bucket_level_access = true

  public_access_prevention = "enforced"

  versioning {
    enabled = true
  }

  force_destroy = false

  labels = {
    owner       = "data-platform"
    pipeline    = "batch-etl-retail"
    cost-centre = "data-001"
    environment = "bootstrap"
    managed-by  = "terraform"
  }

  lifecycle {
    prevent_destroy = true
  }
}