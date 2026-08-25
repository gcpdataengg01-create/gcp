"""Build the additive fct_sales_line fact in Spark."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


UNKNOWN_KEY = -1
INVALID_KEY = -2

FACT_COLUMNS = [
    "source_txn_id",
    "invoice",
    "stock_code",
    "invoice_date_local",
    "date_key",
    "customer_key",
    "product_key",
    "customer_id",
    "country_code",
    "quantity",
    "price",
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
    "run_id",
    "loaded_at",
]


def build_sales_fact(
    clean_sales_df: DataFrame,
    date_dim_df: DataFrame,
    customer_dim_df: DataFrame,
    product_dim_df: DataFrame,
    business_date: str,
    run_id: str,
) -> DataFrame:
    """Join one clean business-date batch to all dimensions.

    Product joins use the SCD2 validity range, never ``is_current=true``.
    Customer is Type 1. Missing anonymous customers route to -1; a present
    customer ID that unexpectedly misses the dimension routes to -2. An
    unseen non-null StockCode routes to -1 as required by C6-001.
    """

    sales = clean_sales_df.filter(
        F.col("invoice_date_local") == F.lit(business_date).cast("date")
    ).alias("s")

    input_rows = sales.count()

    dates = date_dim_df.select(
        F.col("calendar_date").alias("_calendar_date"),
        F.col("date_key").alias("_date_key"),
    ).alias("d")

    with_date = sales.join(
        dates,
        F.col("s.invoice_date_local") == F.col("d._calendar_date"),
        "left",
    )

    customers = (
        customer_dim_df.filter(F.col("customer_key") > 0)
        .select(
            F.col("customer_id").alias("_customer_id"),
            F.col("customer_key").alias("_customer_key"),
        )
        .alias("c")
    )

    with_customer = with_date.join(
        customers,
        F.col("s.customer_id") == F.col("c._customer_id"),
        "left",
    )

    products = (
        product_dim_df.filter(F.col("product_key") > 0)
        .select(
            F.col("stock_code").alias("_product_stock_code"),
            F.col("product_key").alias("_product_key"),
            F.col("valid_from").alias("_valid_from"),
            F.col("valid_to").alias("_valid_to"),
        )
        .alias("p")
    )

    product_condition = (
        (F.col("s.stock_code") == F.col("p._product_stock_code"))
        & (F.col("s.invoice_date_local") >= F.col("p._valid_from"))
        & (F.col("s.invoice_date_local") <= F.col("p._valid_to"))
    )

    joined = with_customer.join(products, product_condition, "left")

    output_rows = joined.count()
    if output_rows != input_rows:
        raise RuntimeError(
            "Dimension joins changed fact grain: "
            f"input_rows={input_rows}, output_rows={output_rows}"
        )

    missing_date = joined.filter(F.col("_date_key").isNull()).limit(1).count()
    if missing_date:
        raise RuntimeError(
            "dim_date does not contain the business date required by the fact"
        )

    result = joined.select(
        F.col("s.txn_id").cast("long").alias("source_txn_id"),
        F.col("s.invoice").cast("string").alias("invoice"),
        F.col("s.stock_code").cast("string").alias("stock_code"),
        F.col("s.invoice_date_local").cast("date").alias("invoice_date_local"),
        F.col("_date_key").cast("long").alias("date_key"),
        F.when(F.col("s.customer_id").isNull(), F.lit(UNKNOWN_KEY))
        .otherwise(F.coalesce(F.col("_customer_key"), F.lit(INVALID_KEY)))
        .cast("long")
        .alias("customer_key"),
        F.when(F.col("s.stock_code").isNull(), F.lit(INVALID_KEY))
        .otherwise(F.coalesce(F.col("_product_key"), F.lit(UNKNOWN_KEY)))
        .cast("long")
        .alias("product_key"),
        F.col("s.customer_id").cast("string").alias("customer_id"),
        F.col("s.country_code").cast("string").alias("country_code"),
        F.col("s.quantity_int").cast("long").alias("quantity"),
        F.col("s.price_decimal").cast("decimal(18,4)").alias("price"),
        F.col("s.line_amount_gbp").cast("decimal(20,2)").alias("line_amount_gbp"),
        F.col("s.line_amount_eur").cast("decimal(20,2)").alias("line_amount_eur"),
        F.col("s.fx_rate").cast("decimal(18,8)").alias("fx_rate"),
        F.col("s.fx_rate_date").cast("date").alias("fx_rate_date"),
        F.col("s.fx_rate_is_carried").cast("boolean").alias("fx_rate_is_carried"),
        F.col("s.currency").cast("string").alias("currency"),
        F.col("s.line_type").cast("string").alias("line_type"),
        F.col("s.is_cancellation").cast("boolean").alias("is_cancellation"),
        F.col("s.is_return_adjustment").cast("boolean").alias("is_return_adjustment"),
        F.col("s.is_free_item").cast("boolean").alias("is_free_item"),
        F.col("s.is_extreme_outlier").cast("boolean").alias("is_extreme_outlier"),
        F.lit(run_id).cast("string").alias("run_id"),
        F.current_timestamp().alias("loaded_at"),
    )

    return result.select(*FACT_COLUMNS)
