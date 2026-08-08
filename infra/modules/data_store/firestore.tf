# =========================================================
# FIRESTORE WATERMARK STORE
# =========================================================

resource "google_firestore_database" "watermark" {
  project     = var.project_id
  name        = "(default)"
  location_id = var.region
  type        = "FIRESTORE_NATIVE"
}

resource "google_project_iam_member" "dataproc_firestore_user" {
  project = var.project_id
  role    = "roles/datastore.user"

  member = "serviceAccount:${var.dataproc_service_account_email}"
}