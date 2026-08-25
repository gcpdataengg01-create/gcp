from datetime import date
from decimal import Decimal

from retail_cleansing.coerce import (
    apply_type_coercion,
)


DATE_FORMATS = [
    "M/d/yyyy H:mm",
]


def test_c3_numeric_and_date_conversion(spark):
    df = spark.createDataFrame(
        [
            (
                "C12345",
                "£1,234.50",
                "(2)",
                "3/14/2011 8:05",
            )
        ],
        [
            "invoice",
            "price",
            "quantity",
            "invoice_date",
        ],
    )

    row = (
        apply_type_coercion(
            df,
            DATE_FORMATS,
        )
        .first()
    )

    assert row["invoice"] == "C12345"

    assert row["price_decimal"] == Decimal(
        "1234.5000"
    )

    assert row["quantity_int"] == -2

    assert row["invoice_date_local"] == date(
        2011,
        3,
        14,
    )

    assert row["invoice_timezone"] == "Europe/London"

    assert row["_dq_c3_price_invalid"] is False
    assert row["_dq_c3_quantity_invalid"] is False
    assert row["_dq_c3_invoice_date_invalid"] is False


def test_c3_invalid_price_is_flagged(spark):
    df = spark.createDataFrame(
        [
            (
                "12345",
                "NOT_A_PRICE",
                "2",
                "3/14/2011 8:05",
            )
        ],
        [
            "invoice",
            "price",
            "quantity",
            "invoice_date",
        ],
    )

    row = (
        apply_type_coercion(
            df,
            DATE_FORMATS,
        )
        .first()
    )

    assert row["price_decimal"] is None
    assert row["_dq_c3_price_invalid"] is True


def test_c3_invalid_date_is_flagged(spark):
    df = spark.createDataFrame(
        [
            (
                "12345",
                "10.00",
                "2",
                "BAD_DATE",
            )
        ],
        [
            "invoice",
            "price",
            "quantity",
            "invoice_date",
        ],
    )

    row = (
        apply_type_coercion(
            df,
            DATE_FORMATS,
        )
        .first()
    )

    assert row["invoice_ts_local"] is None
    assert row["_dq_c3_invoice_date_invalid"] is True