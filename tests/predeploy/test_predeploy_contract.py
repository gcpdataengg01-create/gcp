from pathlib import Path

from retail_fact.quality import validate_quarantine_rate


def text(path):
    return Path(path).read_text(encoding="utf-8")


def test_quarantine_threshold_exactly_two_percent_passes():
    assert validate_quarantine_rate(1000, 20)["passed"] is True


def test_quarantine_threshold_above_two_percent_fails():
    assert validate_quarantine_rate(1000, 21)["passed"] is False


def test_raw_lifecycle_is_configured():
    storage = text("infra/modules/storage/main.tf")
    assert "raw_lifecycle_age_days" in storage
    assert 'enabled = each.key == "raw"' in storage


def test_single_fx_raw_iam_owner():
    compute = text("infra/modules/compute/main.tf")
    storage = text("infra/modules/storage/main.tf")
    assert 'resource "google_storage_bucket_iam_member" "fx_raw_writer"' not in compute
    assert 'resource "google_storage_bucket_iam_member" "fx_raw_writer"' in storage


def test_composer_is_explicit_and_has_firestore_dependency():
    variables = text("infra/envs/dev/variables.tf")
    orchestration = text("infra/modules/orchestration/main.tf")
    assert "composer-3-airflow-" in variables
    assert "pypi_packages = var.composer_pypi_packages" in orchestration


def test_managed_spark_dependency_zip_is_wired_to_every_batch():
    dag = text("dags/retail_batch_etl.py")
    deploy = text("scripts/module12/deploy_runtime.ps1")
    assert 'runtime_py_files = ["packages/runtime_deps.zip"]' in dag
    assert "runtime_deps.zip" in deploy


def test_g20_cost_view_exists():
    warehouse = text("infra/modules/warehouse/main.tf")
    assert 'table_id   = "v_batch_etl_query_usage"' in warehouse
    assert "INFORMATION_SCHEMA.JOBS_BY_PROJECT" in warehouse


def test_c5_exact_duplicate_flag_is_not_concatenated():
    entity = text("jobs/entity/spark_entity_c5_c7.py")
    assert "_dq_c5_exact_duplicate_dq_c5_exact_duplicate" not in entity
