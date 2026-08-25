locals {
  required_apis = toset([
    "compute.googleapis.com",
    "sqladmin.googleapis.com",
    "storage.googleapis.com",
    "dataproc.googleapis.com",
    "bigquery.googleapis.com",
    "firestore.googleapis.com",
    "secretmanager.googleapis.com",
    "cloudkms.googleapis.com",
    "run.googleapis.com",
    "cloudscheduler.googleapis.com",
    "composer.googleapis.com",
    "dataplex.googleapis.com",
    "datacatalog.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
    "serviceusage.googleapis.com",
    "vpcaccess.googleapis.com",
    "servicenetworking.googleapis.com",
    "artifactregistry.googleapis.com"
  ])
}

resource "google_project_service" "required" {
  for_each = local.required_apis

  project = var.project_id
  service = each.value

  disable_on_destroy = false
}