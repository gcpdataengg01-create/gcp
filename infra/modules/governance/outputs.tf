output "dataplex_lake_name" {
  value = google_dataplex_lake.retail.name
}

output "dataplex_quality_scan_name" {
  value = google_dataplex_datascan.c9_quality.name
}

output "semantic_dataset_id" {
  value = google_bigquery_dataset.semantic.dataset_id
}

output "semantic_sales_view_id" {
  value = google_bigquery_table.sales_view.id
}
