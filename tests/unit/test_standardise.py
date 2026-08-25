from retail_cleansing.standardise import (
    apply_domain_standardisation,
)


def test_c4_country_mapping(spark):
    df = spark.createDataFrame(
        [
            (
                "France",
                "POST",
            )
        ],
        [
            "country",
            "stock_code",
        ],
    )

    row = (
        apply_domain_standardisation(
            df,
            country_map={
                "France": "FR",
            },
            stock_code_types={
                "POST": {
                    "line_type": "shipping",
                }
            },
        )
        .first()
    )

    assert row["country_code"] == "FR"
    assert row["_dq_c4_country_unknown"] is False

    assert row["line_type"] == "shipping"

    assert row["currency"] == "GBP"


def test_c4_unknown_country_is_preserved(spark):
    df = spark.createDataFrame(
        [
            (
                "Atlantis",
                "ABC123",
            )
        ],
        [
            "country",
            "stock_code",
        ],
    )

    row = (
        apply_domain_standardisation(
            df,
            country_map={
                "France": "FR",
            },
            stock_code_types={
                "POST": {
                    "line_type": "shipping",
                }
            },
        )
        .first()
    )

    assert row["country_code"] is None
    assert row["_dq_c4_country_unknown"] is True

    # Unknown code must not disappear.
    assert row["stock_code"] == "ABC123"

    # Default code classification.
    assert row["line_type"] == "product"


def test_c4_test_stockcode_is_flagged(spark):
    df = spark.createDataFrame(
        [
            (
                "United Kingdom",
                "TEST001",
            )
        ],
        [
            "country",
            "stock_code",
        ],
    )

    row = (
        apply_domain_standardisation(
            df,
            country_map={
                "United Kingdom": "GB",
            },
            stock_code_types={
                "TEST001": {
                    "line_type": "test",
                }
            },
        )
        .first()
    )

    assert row["line_type"] == "test"
    assert row["_dq_c4_test_line"] is True