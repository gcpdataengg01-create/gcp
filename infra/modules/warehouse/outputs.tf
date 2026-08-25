output "curated_dataset_id" {
  value = google_bigquery_dataset.curated.dataset_id
}

output "staging_dataset_id" {
  value = google_bigquery_dataset.staging.dataset_id
}

output "fct_sales_line_table_id" {
  value = google_bigquery_table.fct_sales_line.id
}

output "maximum_bytes_billed" {
  value = var.maximum_bytes_billed
}
