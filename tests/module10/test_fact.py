from datetime import date, datetime
from decimal import Decimal

from retail_fact.build import build_sales_fact


def _clean_sales(spark, invoice_date):
    return spark.createDataFrame(
        [
            (
                101,
                "INV-1",
                "SKU-1",
                invoice_date,
                "CUST-1",
                "GB",
                2,
                Decimal("5.0000"),
                Decimal("10.00"),
                Decimal("11.50"),
                Decimal("1.15000000"),
                invoice_date,
                False,
                "GBP",
                "product",
                False,
                False,
                False,
                False,
            )
        ],
        [
            "txn_id",
            "invoice",
            "stock_code",
            "invoice_date_local",
            "customer_id",
            "country_code",
            "quantity_int",
            "price_decimal",
            "line_amount_gbp",
            "line_amount_eur",
            "fx_rate",
            "fx_rate_date",
            "fx_rate_is_carried",
            "currency",
            "line_type",
            "is_cancellation",
            "is_return_adjustment",
            "is_free_item",
            "is_extreme_outlier",
        ],
    )


def test_fact_joins_customer_date_and_historical_product(spark):
    business_date = date(2011, 3, 14)
    clean = _clean_sales(spark, business_date)

    dim_date = spark.createDataFrame(
        [(20110314, business_date)],
        ["date_key", "calendar_date"],
    )
    dim_customer = spark.createDataFrame(
        [(9001, "CUST-1")],
        ["customer_key", "customer_id"],
    )
    dim_product = spark.createDataFrame(
        [
            (7001, "SKU-1", date(2011, 1, 1), date(2011, 3, 13)),
            (7002, "SKU-1", date(2011, 3, 14), date(9999, 12, 31)),
        ],
        ["product_key", "stock_code", "valid_from", "valid_to"],
    )

    row = build_sales_fact(
        clean,
        dim_date,
        dim_customer,
        dim_product,
        "2011-03-14",
        "run-1",
    ).first()

    assert row["date_key"] == 20110314
    assert row["customer_key"] == 9001
    assert row["product_key"] == 7002
    assert row["quantity"] == 2
    assert row["price"] == Decimal("5.0000")
    assert row["line_amount_gbp"] == Decimal("10.00")
    assert row["run_id"] == "run-1"


def test_fact_routes_anonymous_customer_to_unknown(spark):
    business_date = date(2011, 3, 14)
    clean = _clean_sales(spark, business_date).drop("customer_id").withColumn(
        "customer_id", __import__("pyspark").sql.functions.lit(None).cast("string")
    )

    dim_date = spark.createDataFrame(
        [(20110314, business_date)], ["date_key", "calendar_date"]
    )
    dim_customer = spark.createDataFrame(
        [(9001, "OTHER")], ["customer_key", "customer_id"]
    )
    dim_product = spark.createDataFrame(
        [(7002, "SKU-1", date(2011, 3, 14), date(9999, 12, 31))],
        ["product_key", "stock_code", "valid_from", "valid_to"],
    )

    row = build_sales_fact(
        clean,
        dim_date,
        dim_customer,
        dim_product,
        "2011-03-14",
        "run-1",
    ).first()

    assert row["customer_key"] == -1
