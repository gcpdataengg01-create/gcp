output "dashboard_id" {
  value = google_monitoring_dashboard.etl.id
}

output "alert_policy_ids" {
  value = {
    for key, policy in google_monitoring_alert_policy.etl : key => policy.id
  }
}
