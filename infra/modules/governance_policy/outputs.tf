output "taxonomy_name" {
  value = google_data_catalog_taxonomy.retail_sensitivity.name
}

output "customer_policy_tag_name" {
  value = google_data_catalog_policy_tag.pseudonymous_customer_identifier.name
}
