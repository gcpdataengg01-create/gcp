from retail_cleansing.quality import (
    validate_expected_columns,
    validate_row_count_range,
    evaluate_quarantine_threshold,
)


def test_c1_expected_columns(spark):
    df = spark.createDataFrame(
        [
            (
                1,
                "100",
                "ABC",
                "Product",
                2,
                "2011-03-14",
                "10.00",
                "123",
                "United Kingdom",
            )
        ],
        [
            "txn_id",
            "invoice",
            "stock_code",
            "description",
            "quantity",
            "invoice_date",
            "price",
            "customer_id",
            "country",
        ],
    )

    # Should not raise.
    validate_expected_columns(df)


def test_c1_row_count_within_expected_range():
    result = validate_row_count_range(
        current_count=100,
        trailing_same_weekday_average=100,
    )

    assert result["status"] == "PASS"


def test_c1_row_count_outside_expected_range():
    result = validate_row_count_range(
        current_count=400,
        trailing_same_weekday_average=100,
    )

    assert result["status"] == "HOLD"


def test_quarantine_exactly_two_percent_is_allowed():
    result = evaluate_quarantine_threshold(
        extracted_rows=1000,
        quarantined_source_rows=20,
    )

    assert (
        result["threshold_exceeded"]
        is False
    )


def test_quarantine_above_two_percent_is_exceeded():
    result = evaluate_quarantine_threshold(
        extracted_rows=1000,
        quarantined_source_rows=21,
    )

    assert (
        result["threshold_exceeded"]
        is True
    )