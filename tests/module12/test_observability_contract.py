from pathlib import Path

TEXT = Path("infra/modules/observability/main.tf").read_text(encoding="utf-8")


def test_all_required_metrics_declared():
    for metric in [
        "rows_extracted",
        "rows_quarantined",
        "repairs_per_rule",
        "control_total_variance",
        "duration_seconds",
    ]:
        assert metric in TEXT


def test_alert_policy_created_for_each_required_signal():
    for key in [
        "rows_extracted",
        "rows_quarantined",
        "repairs_per_rule",
        "control_total_variance",
        "duration",
    ]:
        assert key in TEXT
    assert 'resource "google_monitoring_alert_policy" "etl"' in TEXT
    assert 'resource "google_monitoring_dashboard" "etl"' in TEXT
