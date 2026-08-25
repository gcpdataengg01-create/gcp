resource "google_data_catalog_taxonomy" "retail_sensitivity" {
  project      = var.project_id
  region       = var.region
  display_name = "retail-etl-${var.environment}-${var.project_id}-sensitivity"
  description  = "Retail ETL data-classification taxonomy for governed warehouse columns."

  activated_policy_types = ["FINE_GRAINED_ACCESS_CONTROL"]
}

resource "google_data_catalog_policy_tag" "pseudonymous_customer_identifier" {
  taxonomy     = google_data_catalog_taxonomy.retail_sensitivity.id
  display_name = "Pseudonymous Customer Identifier"
  description  = "Pseudonymous customer identifiers such as customer_id."
}
