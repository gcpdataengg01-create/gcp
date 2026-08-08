output "network_id" {
  value = google_compute_network.etl_vpc.id
}

output "network_name" {
  value = google_compute_network.etl_vpc.name
}

output "subnet_id" {
  value = google_compute_subnetwork.etl_subnet.id
}

output "subnet_name" {
  value = google_compute_subnetwork.etl_subnet.name
}

output "serverless_connector_id" {
  value = google_vpc_access_connector.serverless_connector.id
}