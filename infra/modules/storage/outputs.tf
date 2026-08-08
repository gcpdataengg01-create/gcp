output "bucket_names" {
  description = "Names of ETL storage-zone buckets"

  value = {
    for key, bucket in google_storage_bucket.zone :
    key => bucket.name
  }
}

output "raw_bucket_name" {
  value = google_storage_bucket.zone["raw"].name
}

output "stage_bucket_name" {
  value = google_storage_bucket.zone["stage"].name
}

output "curated_bucket_name" {
  value = google_storage_bucket.zone["curated"].name
}

output "quarantine_bucket_name" {
  value = google_storage_bucket.zone["quarantine"].name
}