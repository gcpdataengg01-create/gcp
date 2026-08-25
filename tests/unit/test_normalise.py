from retail_cleansing.normalise import (
    normalise_sales,
    apply_operational_description_rules,
)


def test_c2_trim_whitespace_and_nbsp(spark):
    df = spark.createDataFrame(
        [
            (
                "  Product\u00A0   Name\t",
                "123",
                "  United Kingdom  ",
            )
        ],
        [
            "description",
            "customer_id",
            "country",
        ],
    )

    row = normalise_sales(
        df,
        sentinel_values=["N/A", "NULL"],
    ).first()

    assert row["description"] == "Product Name"
    assert row["customer_id"] == "123"
    assert row["country"] == "United Kingdom"


def test_c2_sentinel_becomes_null(spark):
    df = spark.createDataFrame(
        [
            (
                "Product",
                "N/A",
                "France",
            )
        ],
        [
            "description",
            "customer_id",
            "country",
        ],
    )

    row = normalise_sales(
        df,
        sentinel_values=["N/A"],
    ).first()

    assert row["customer_id"] is None


def test_c2_005_operational_description(spark):
    df = spark.createDataFrame(
        [
            (" Manual Adjustment ",)
        ],
        ["description"],
    )

    row = (
        apply_operational_description_rules(
            df,
            ["MANUAL ADJUSTMENT"],
        )
        .first()
    )

    assert row["description"] is None
    assert row["is_adjustment"] is True