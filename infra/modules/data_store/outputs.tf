output "instance_name" {
  value = google_sql_database_instance.postgres.name
}

output "private_ip_address" {
  value = google_sql_database_instance.postgres.private_ip_address
}

output "database_name" {
  value = google_sql_database.retail.name
}

output "database_user" {
  value = google_sql_user.etl.name
}

output "connection_name" {
  value = google_sql_database_instance.postgres.connection_name
}

output "firestore_database_name" {
  value = google_firestore_database.watermark.name
}