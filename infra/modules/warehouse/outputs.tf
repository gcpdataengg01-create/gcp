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

output "ops_dataset_id" {
  value = google_bigquery_dataset.ops.dataset_id
}

output "batch_control_table_id" {
  value = google_bigquery_table.etl_batch_control.id
}

output "fct_sales_line_staging_table_id" {
  value = google_bigquery_table.fct_sales_line_stg.id
}


output "query_usage_view_id" {
  value = google_bigquery_table.batch_etl_query_usage.id
}
