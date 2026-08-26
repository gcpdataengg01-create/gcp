locals {
  metric_prefix = "retail_etl"
}

resource "google_logging_metric" "rows_extracted" {
  project     = var.project_id
  name        = "${local.metric_prefix}/rows_extracted"
  description = "Rows extracted for each retail ETL run."
  filter      = "textPayload:\"MODULE12_RUN_METRIC\""

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "DISTRIBUTION"
    unit        = "1"
  }

  value_extractor = "REGEXP_EXTRACT(textPayload, \"\\\"rows_extracted\\\": ([0-9]+)\")"

  bucket_options {
    exponential_buckets {
      num_finite_buckets = 20
      growth_factor      = 2
      scale              = 1
    }
  }
}

resource "google_logging_metric" "rows_quarantined" {
  project     = var.project_id
  name        = "${local.metric_prefix}/rows_quarantined"
  description = "Rows quarantined for each retail ETL run."
  filter      = "textPayload:\"MODULE12_RUN_METRIC\""

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "DISTRIBUTION"
    unit        = "1"
  }

  value_extractor = "REGEXP_EXTRACT(textPayload, \"\\\"rows_quarantined\\\": ([0-9]+)\")"

  bucket_options {
    exponential_buckets {
      num_finite_buckets = 20
      growth_factor      = 2
      scale              = 1
    }
  }
}

resource "google_logging_metric" "control_total_variance" {
  project     = var.project_id
  name        = "${local.metric_prefix}/control_total_variance"
  description = "Absolute GBP source-to-mart control-total variance."
  filter      = "textPayload:\"MODULE12_RUN_METRIC\""

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "DISTRIBUTION"
    unit        = "1"
  }

  value_extractor = "REGEXP_EXTRACT(textPayload, \"\\\"control_total_variance\\\": ([0-9.]+)\")"

  bucket_options {
    exponential_buckets {
      num_finite_buckets = 20
      growth_factor      = 2
      scale              = 0.001
    }
  }
}

resource "google_logging_metric" "duration" {
  project     = var.project_id
  name        = "${local.metric_prefix}/duration_seconds"
  description = "End-to-end DAG duration in seconds."
  filter      = "textPayload:\"MODULE12_RUN_METRIC\""

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "DISTRIBUTION"
    unit        = "s"
  }

  value_extractor = "REGEXP_EXTRACT(textPayload, \"\\\"duration_seconds\\\": ([0-9.]+)\")"

  bucket_options {
    exponential_buckets {
      num_finite_buckets = 20
      growth_factor      = 2
      scale              = 1
    }
  }
}

resource "google_logging_metric" "repairs_per_rule" {
  project     = var.project_id
  name        = "${local.metric_prefix}/repairs_per_rule"
  description = "Deterministic repairs by cleansing rule."
  filter      = "textPayload:\"MODULE12_REPAIR_METRIC\""

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "DISTRIBUTION"
    unit        = "1"

    labels {
      key         = "rule_id"
      value_type  = "STRING"
      description = "Cleansing rule identifier"
    }
  }

  value_extractor = "REGEXP_EXTRACT(textPayload, \"\\\"repairs\\\": ([0-9]+)\")"
  label_extractors = {
    rule_id = "REGEXP_EXTRACT(textPayload, \"\\\"rule_id\\\": \\\"([^\\\"]+)\\\"\")"
  }

  bucket_options {
    exponential_buckets {
      num_finite_buckets = 20
      growth_factor      = 2
      scale              = 1
    }
  }
}

locals {
  alert_metrics = {
    rows_extracted = {
      metric     = google_logging_metric.rows_extracted.name
      comparison = "COMPARISON_LT"
      threshold  = 1
      display    = "Retail ETL rows extracted is zero"
    }
    rows_quarantined = {
      metric     = google_logging_metric.rows_quarantined.name
      comparison = "COMPARISON_GT"
      threshold  = 0
      display    = "Retail ETL quarantine activity"
    }
    repairs_per_rule = {
      metric     = google_logging_metric.repairs_per_rule.name
      comparison = "COMPARISON_GT"
      threshold  = 0
      display    = "Retail ETL repair activity"
    }
    control_total_variance = {
      metric     = google_logging_metric.control_total_variance.name
      comparison = "COMPARISON_GT"
      threshold  = 0.01
      display    = "Retail ETL control total variance"
    }
    duration = {
      metric     = google_logging_metric.duration.name
      comparison = "COMPARISON_GT"
      threshold  = 1800
      display    = "Retail ETL duration regression"
    }
  }
}

resource "google_monitoring_alert_policy" "etl" {
  for_each = local.alert_metrics
  project  = var.project_id

  display_name          = each.value.display
  combiner              = "OR"
  enabled               = true
  notification_channels = var.notification_channels

  conditions {
    display_name = each.value.display

    condition_threshold {
      filter          = "metric.type=\"logging.googleapis.com/user/${each.value.metric}\""
      comparison      = each.value.comparison
      threshold_value = each.value.threshold
      duration        = "0s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_PERCENTILE_50"
      }
    }
  }
}

resource "google_monitoring_dashboard" "etl" {
  project = var.project_id
  dashboard_json = jsonencode({
    displayName = "Retail Batch ETL - ${var.environment}"
    mosaicLayout = {
      columns = 12
      tiles = [
        for index, metric in [
          "rows_extracted",
          "rows_quarantined",
          "repairs_per_rule",
          "control_total_variance",
          "duration_seconds"
          ] : {
          xPos   = (index % 2) * 6
          yPos   = floor(index / 2) * 4
          width  = 6
          height = 4
          widget = {
            title = metric
            xyChart = {
              dataSets = [{
                timeSeriesQuery = {
                  timeSeriesFilter = {
                    filter = "metric.type=\"logging.googleapis.com/user/${local.metric_prefix}/${metric}\""
                    aggregation = {
                      alignmentPeriod  = "300s"
                      perSeriesAligner = "ALIGN_PERCENTILE_50"
                    }
                  }
                }
                plotType = "LINE"
              }]
            }
          }
        }
      ]
    }
  })
}
