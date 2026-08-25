from retail_cleansing.quarantine import (
    build_quarantine_records,
    split_valid_and_quarantine,
)


def test_quarantine_record_schema(spark):
    df = spark.createDataFrame(
        [
            (
                1,
                "100",
                "ABC",
                0,
                True,
            )
        ],
        [
            "txn_id",
            "invoice",
            "stock_code",
            "quantity",
            "_dq_c7_zero_quantity",
        ],
    )

    quarantine_df = build_quarantine_records(
        df=df,
        run_id="unit-test-run",
        business_date="2011-03-14",
    )

    row = quarantine_df.first()

    assert row["run_id"] == "unit-test-run"
    assert str(row["business_date"]) == "2011-03-14"
    assert row["source_system"] == "retail_db"
    assert row["entity"] == "sales_txn"
    assert row["rule_id"] == "C7-003"
    assert row["rule_severity"] == "QUARANTINE"
    assert row["column_name"] == "quantity"

    assert row["raw_payload"] is not None
    assert row["detected_at"] is not None

    assert row["reprocessed_at"] is None
    assert row["reprocess_run_id"] is None


def test_quarantine_split_has_no_silent_drop(spark):
    df = spark.createDataFrame(
        [
            (
                1,
                False,
            ),
            (
                2,
                True,
            ),
        ],
        [
            "txn_id",
            "_dq_c7_zero_quantity",
        ],
    )

    valid_df, rejected_df = (
        split_valid_and_quarantine(df)
    )

    assert valid_df.count() == 1
    assert rejected_df.count() == 1

    assert (
        valid_df.count()
        + rejected_df.count()
        == df.count()
    )