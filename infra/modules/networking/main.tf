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


resource "google_compute_firewall" "allow_internal" {
  name    = "${var.environment}-allow-internal"
  network = google_compute_network.etl_vpc.name
  project = var.project_id

  direction = "INGRESS"

  source_ranges = [
    "10.10.0.0/24"
  ]

  allow {
    protocol = "tcp"
    ports = [
      "0-65535"
    ]
  }

  allow {
    protocol = "udp"
    ports = [
      "0-65535"
    ]
  }

  allow {
    protocol = "icmp"
  }
}

resource "google_compute_firewall" "allow_https_egress" {
  name    = "${var.environment}-allow-https-egress"
  network = google_compute_network.etl_vpc.name
  project = var.project_id

  direction = "EGRESS"

  destination_ranges = [
    "0.0.0.0/0"
  ]

  allow {
    protocol = "tcp"
    ports = [
      "443"
    ]
  }
}