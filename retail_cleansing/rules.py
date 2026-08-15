from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


MONEY_TYPE = DecimalType(20, 2)
FX_RATE_TYPE = DecimalType(18, 8)


# ============================================================
# C6 - Referential Integrity
# ============================================================


def apply_unknown_reference_flags(
    df: DataFrame,
) -> DataFrame:
    """
    C6-002 / C6-003

    Missing references are flagged and preserved.

    Actual surrogate keys (-1 / inferred members) are assigned
    when dimensions are built in Module 9.
    """

    df = df.withColumn(
        "_dq_c6_customer_unknown",
        F.col("customer_id").isNull(),
    )

    df = df.withColumn(
        "_dq_c6_country_unknown",
        F.col("country_code").isNull(),
    )

    return df


def flag_unseen_products(
    fact_df: DataFrame,
    product_df: DataFrame,
) -> DataFrame:
    """
    C6-001

    Identify StockCodes not found in the product reference.

    These rows are retained and will create/use an inferred
    product member in Module 9.
    """

    product_keys = (
        product_df
        .select("stock_code")
        .dropDuplicates()
        .withColumn(
            "_product_exists",
            F.lit(True),
        )
    )

    before = fact_df.count()

    joined = fact_df.join(
        product_keys,
        on="stock_code",
        how="left",
    )

    after = joined.count()

    if before != after:
        raise RuntimeError(
            "C6 referential join changed the fact row count: "
            f"before={before}, after={after}"
        )

    return (
        joined
        .withColumn(
            "_dq_c6_product_stub_required",
            F.col("_product_exists").isNull(),
        )
        .drop("_product_exists")
    )


# ============================================================
# C7 - Business Rules
# ============================================================


def apply_cancellation_rules(
    df: DataFrame,
) -> DataFrame:
    """
    C7-001 / C7-002 / C7-010

    Invoice starting C = cancellation.

    Cancellation quantities must remain negative.
    Non-C negative quantities are retained and flagged as
    returns/adjustments.
    """

    is_cancellation = (
        F.upper(
            F.trim(F.col("invoice"))
        ).startswith("C")
    )

    df = df.withColumn(
        "is_cancellation",
        is_cancellation,
    )

    # Enforce negative quantity for cancellation.
    df = df.withColumn(
        "quantity_int",
        F.when(
            F.col("is_cancellation")
            & (F.col("quantity_int") > 0),
            -F.abs(F.col("quantity_int")),
        ).otherwise(
            F.col("quantity_int")
        ),
    )

    df = df.withColumn(
        "is_return_adjustment",
        (
            ~F.col("is_cancellation")
            & (F.col("quantity_int") < 0)
        ),
    )

    return df


def apply_quantity_rules(
    df: DataFrame,
) -> DataFrame:
    """
    C7-003

    Quantity zero -> quarantine.
    """

    return df.withColumn(
        "_dq_c7_zero_quantity",
        F.col("quantity_int") == 0,
    )


def apply_price_rules(
    df: DataFrame,
) -> DataFrame:
    """
    C7-004 / C7-005

    price == 0:
        retain and mark free/sample

    price < 0:
        allowed only for adjustment lines
    """

    df = df.withColumn(
        "is_free_item",
        F.col("price_decimal") == 0,
    )

    df = df.withColumn(
        "_dq_c7_negative_price_invalid",
        (
            (F.col("price_decimal") < 0)
            & (
                F.col("line_type")
                != F.lit("adjustment")
            )
        ),
    )

    return df


def calculate_line_amount(
    df: DataFrame,
) -> DataFrame:
    """
    C7-006

    line_amount = quantity * price
    explicitly rounded to 2 decimal places.

    GBP remains the source financial currency.
    """

    amount = (
        F.col("quantity_int")
        * F.col("price_decimal")
    )

    return df.withColumn(
        "line_amount_gbp",
        F.bround(
            amount,
            2,
        ).cast(MONEY_TYPE),
    )


def apply_outlier_flags(
    df: DataFrame,
    quantity_abs_threshold: Optional[int] = None,
    line_amount_abs_threshold: Optional[float] = None,
) -> DataFrame:
    """
    C7-007

    Outliers must be flagged, never corrected or dropped.

    The supplied requirements do not prescribe numerical
    thresholds, so thresholds are configuration-driven and
    will be finalized from source profiling evidence.
    """

    condition = F.lit(False)

    if quantity_abs_threshold is not None:
        condition = (
            condition
            | (
                F.abs(F.col("quantity_int"))
                > F.lit(quantity_abs_threshold)
            )
        )

    if line_amount_abs_threshold is not None:
        condition = (
            condition
            | (
                F.abs(F.col("line_amount_gbp"))
                > F.lit(line_amount_abs_threshold)
            )
        )

    return df.withColumn(
        "is_extreme_outlier",
        condition,
    )


def join_fx_rates(
    sales_df: DataFrame,
    fx_df: DataFrame,
) -> DataFrame:
    """
    C7-008 / C7-009

    Join cached GBP/EUR FX reference rates using the invoice
    business date.

    Expected FX columns:
        requested_date
        fx_rate_date
        fx_rate
        fx_rate_is_carried
        base
        quote
    """

    fx_reference = (
        fx_df
        .select(
            F.to_date(
                F.col("requested_date")
            ).alias("requested_date"),
            F.to_date(
                F.col("fx_rate_date")
            ).alias("fx_rate_date"),
            F.col("fx_rate")
            .cast(FX_RATE_TYPE)
            .alias("fx_rate"),
            F.col(
                "fx_rate_is_carried"
            ).cast("boolean"),
            F.col("base").alias("fx_base"),
            F.col("quote").alias("fx_quote"),
        )
        .dropDuplicates(
            ["requested_date"]
        )
    )

    before = sales_df.count()

    result = sales_df.join(
        F.broadcast(fx_reference),
        sales_df["invoice_date_local"]
        == fx_reference["requested_date"],
        "left",
    )

    after = result.count()

    if before != after:
        raise RuntimeError(
            "FX join changed sales row count: "
            f"before={before}, after={after}"
        )

    result = result.drop(
        "requested_date"
    )

    result = result.withColumn(
        "_dq_c7_fx_missing",
        (
            F.col("fx_rate").isNull()
            | F.col("fx_rate_date").isNull()
        ),
    )

    result = result.withColumn(
        "line_amount_eur",
        F.when(
            ~F.col("_dq_c7_fx_missing"),
            F.bround(
                F.col("line_amount_gbp")
                * F.col("fx_rate"),
                2,
            ).cast(MONEY_TYPE),
        ),
    )

    return result


def apply_business_rules(
    df: DataFrame,
    quantity_abs_threshold: Optional[int] = None,
    line_amount_abs_threshold: Optional[float] = None,
) -> DataFrame:
    """
    Apply C6 and non-FX C7 rules.
    """

    df = apply_unknown_reference_flags(df)

    df = apply_cancellation_rules(df)

    df = apply_quantity_rules(df)

    df = apply_price_rules(df)

    df = calculate_line_amount(df)

    df = apply_outlier_flags(
        df,
        quantity_abs_threshold=quantity_abs_threshold,
        line_amount_abs_threshold=line_amount_abs_threshold,
    )

    return df