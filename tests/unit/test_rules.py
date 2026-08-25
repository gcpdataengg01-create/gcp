from decimal import Decimal

from retail_cleansing.rules import (
    apply_business_rules,
    join_fx_rates,
)


def test_c6_unknown_customer_and_country(spark):
    df = spark.createDataFrame(
        [
            (
                "100",
                2,
                Decimal("3.0000"),
                "product",
                None,
                None,
            )
        ],
        [
            "invoice",
            "quantity_int",
            "price_decimal",
            "line_type",
            "customer_id",
            "country_code",
        ],
    )

    row = apply_business_rules(df).first()

    assert row["_dq_c6_customer_unknown"] is True
    assert row["_dq_c6_country_unknown"] is True


def test_c7_cancellation_quantity_becomes_negative(spark):
    df = spark.createDataFrame(
        [
            (
                "C100",
                2,
                Decimal("3.0000"),
                "product",
                "123",
                "GB",
            )
        ],
        [
            "invoice",
            "quantity_int",
            "price_decimal",
            "line_type",
            "customer_id",
            "country_code",
        ],
    )

    row = apply_business_rules(df).first()

    assert row["is_cancellation"] is True

    # Cancellations remain in the data and quantity stays negative.
    assert row["quantity_int"] == -2

    assert row["line_amount_gbp"] == Decimal(
        "-6.00"
    )


def test_c7_zero_quantity_is_flagged(spark):
    df = spark.createDataFrame(
        [
            (
                "100",
                0,
                Decimal("3.0000"),
                "product",
                "123",
                "GB",
            )
        ],
        [
            "invoice",
            "quantity_int",
            "price_decimal",
            "line_type",
            "customer_id",
            "country_code",
        ],
    )

    row = apply_business_rules(df).first()

    assert row["_dq_c7_zero_quantity"] is True


def test_c7_zero_price_is_free_item(spark):
    df = spark.createDataFrame(
        [
            (
                "100",
                1,
                Decimal("0.0000"),
                "product",
                "123",
                "GB",
            )
        ],
        [
            "invoice",
            "quantity_int",
            "price_decimal",
            "line_type",
            "customer_id",
            "country_code",
        ],
    )

    row = apply_business_rules(df).first()

    assert row["is_free_item"] is True


def test_c7_negative_non_adjustment_price_is_invalid(spark):
    df = spark.createDataFrame(
        [
            (
                "100",
                1,
                Decimal("-2.0000"),
                "product",
                "123",
                "GB",
            )
        ],
        [
            "invoice",
            "quantity_int",
            "price_decimal",
            "line_type",
            "customer_id",
            "country_code",
        ],
    )

    row = apply_business_rules(df).first()

    assert (
        row["_dq_c7_negative_price_invalid"]
        is True
    )


def test_c7_fx_join(spark):
    sales_df = spark.createDataFrame(
        [
            (
                "2011-03-14",
                Decimal("10.00"),
            )
        ],
        [
            "invoice_date_local",
            "line_amount_gbp",
        ],
    ).selectExpr(
        "cast(invoice_date_local as date) as invoice_date_local",
        "line_amount_gbp",
    )

    fx_df = spark.createDataFrame(
        [
            (
                "2011-03-14",
                "2011-03-14",
                Decimal("1.15000000"),
                False,
                "GBP",
                "EUR",
            )
        ],
        [
            "requested_date",
            "fx_rate_date",
            "fx_rate",
            "fx_rate_is_carried",
            "base",
            "quote",
        ],
    )

    row = join_fx_rates(
        sales_df,
        fx_df,
    ).first()

    assert row["fx_rate"] == Decimal(
        "1.15000000"
    )

    assert row["line_amount_eur"] == Decimal(
        "11.50"
    )

    assert row["_dq_c7_fx_missing"] is False