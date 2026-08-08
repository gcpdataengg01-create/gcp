resource "google_compute_network" "etl_vpc" {
  project                 = var.project_id
  name                    = "retail-etl-${var.environment}-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "etl_subnet" {
  project       = var.project_id
  name          = "retail-etl-${var.environment}-subnet"
  region        = var.region
  network       = google_compute_network.etl_vpc.id
  ip_cidr_range = var.subnet_cidr

  private_ip_google_access = true
}

resource "google_vpc_access_connector" "serverless_connector" {
  project = var.project_id
  name    = "retail-etl-${var.environment}-conn"
  region  = var.region

  network = google_compute_network.etl_vpc.name

  ip_cidr_range = var.connector_cidr

  min_instances = 2
  max_instances = 3

  depends_on = [
    google_compute_subnetwork.etl_subnet
  ]
}