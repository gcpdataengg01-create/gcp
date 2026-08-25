from pathlib import Path

DAG_TEXT = Path("dags/retail_batch_etl.py").read_text(encoding="utf-8")


def test_required_tasks_exist():
    for task in [
        "claim_watermark",
        "extract_sales",
        "extract_fx",
        "cleanse_c1_c4",
        "entity_c5_c7",
        "dim_date",
        "dim_customer",
        "dim_product_scd2",
        "build_fact",
        "load_bq_staging",
        "dataplex_quality",
        "publish",
        "commit_watermark",
        "emit_run_metrics",
    ]:
        assert f'task_id="{task}"' in DAG_TEXT


def test_business_date_is_logical_or_explicit_config():
    assert "dag_run.conf.get('business_date', ds)" in DAG_TEXT
    assert "datetime.now" not in DAG_TEXT
    assert "date.today" not in DAG_TEXT


def test_no_dataset_xcom_contract():
    assert DAG_TEXT.count("do_xcom_push=False") >= 10
    assert "xcom_pull" not in DAG_TEXT


def test_table_by_table_fanout_not_serial_layers():
    assert "claim >> [extract_sales, extract_fx]" in DAG_TEXT
    assert "entity_c5_c7 >> [dim_date, dim_customer, dim_product]" in DAG_TEXT
    assert "[dim_date, dim_customer, dim_product] >> build_fact" in DAG_TEXT
    assert "extract_all" not in DAG_TEXT
    assert "transform_all" not in DAG_TEXT
    assert "load_all" not in DAG_TEXT


def test_quality_precedes_publish_and_commit():
    assert "load_bq_staging >> dataplex_quality >> publish >> commit" in DAG_TEXT
