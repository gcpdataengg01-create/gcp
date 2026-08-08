# =========================================================
# PRIVATE SERVICE ACCESS
# =========================================================

resource "google_compute_global_address" "private_service_range" {
  project       = var.project_id
  name          = "retail-etl-${var.environment}-sql-range"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = var.network_id
}

resource "google_service_networking_connection" "private_vpc_connection" {
  network = var.network_id
  service = "servicenetworking.googleapis.com"

  reserved_peering_ranges = [
    google_compute_global_address.private_service_range.name
  ]
}


# =========================================================
# CLOUD SQL POSTGRESQL
# =========================================================

resource "google_sql_database_instance" "postgres" {
  project          = var.project_id
  name             = "retail-etl-${var.environment}-postgres"
  region           = var.region
  database_version = "POSTGRES_15"

  deletion_protection = false

  settings {
    tier              = "db-f1-micro"
    availability_type = "ZONAL"
    disk_type         = "PD_SSD"
    disk_size         = 10
    disk_autoresize   = true

    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
    }

    ip_configuration {
      ipv4_enabled    = false
      private_network = var.network_id
    }
  }

  depends_on = [
    google_service_networking_connection.private_vpc_connection
  ]
}


# =========================================================
# DATABASE
# =========================================================

resource "google_sql_database" "retail" {
  project  = var.project_id
  name     = "retail"
  instance = google_sql_database_instance.postgres.name
}


# =========================================================
# DATABASE PASSWORD
# =========================================================

resource "random_password" "postgres" {
  length  = 24
  special = true

  override_special = "!#$%&*()-_=+"
}


# =========================================================
# DATABASE USER
# =========================================================

resource "google_sql_user" "etl" {
  project  = var.project_id
  name     = "retail_etl"
  instance = google_sql_database_instance.postgres.name
  password = random_password.postgres.result
}


# =========================================================
# SECRET MANAGER VERSIONS
# Secret containers were created in Module 3.
# =========================================================

resource "google_secret_manager_secret_version" "username" {
  secret      = var.db_username_secret_id
  secret_data = google_sql_user.etl.name

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_secret_manager_secret_version" "password" {
  secret      = var.db_password_secret_id
  secret_data = random_password.postgres.result

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_secret_manager_secret_version" "database" {
  secret      = var.db_database_secret_id
  secret_data = google_sql_database.retail.name

  lifecycle {
    create_before_destroy = true
  }
}

