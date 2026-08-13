from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


PRICE_TYPE = DecimalType(18, 4)


def _clean_numeric_text(column_name: str):
    """
    C3-001

    Normalise numeric text:
    - trim whitespace
    - remove GBP/USD/EUR currency symbols
    - remove thousands separators
    - convert accounting parentheses to negative values
    """

    value = F.trim(F.col(column_name).cast("string"))

    # Detect accounting notation: (123.45) -> -123.45
    is_parenthesized = (
        value.startswith("(")
        & value.endswith(")")
    )

    cleaned = F.regexp_replace(
        value,
        r"[£$€,]",
        "",
    )

    cleaned = F.regexp_replace(
        cleaned,
        r"^\(",
        "",
    )

    cleaned = F.regexp_replace(
        cleaned,
        r"\)$",
        "",
    )

    cleaned = F.trim(cleaned)

    return F.when(
        is_parenthesized,
        F.concat(F.lit("-"), cleaned),
    ).otherwise(cleaned)


def coerce_price(df: DataFrame) -> DataFrame:
    """
    C3-001 / C3-002 / C3-003

    Price must be DECIMAL(18,4), never float/double.

    Invalid or overflowing values are flagged for quarantine.
    """

    df = df.withColumn(
        "_price_numeric_text",
        _clean_numeric_text("price"),
    )

    # try_cast avoids terminating the Spark job for malformed data.
    df = df.withColumn(
        "price_decimal",
        F.expr(
            "try_cast(_price_numeric_text AS DECIMAL(18,4))"
        ),
    )

    df = df.withColumn(
        "_dq_c3_price_invalid",
        (
            F.col("_price_numeric_text").isNotNull()
            & F.col("price_decimal").isNull()
        ),
    )

    return df.drop("_price_numeric_text")


def coerce_quantity(df: DataFrame) -> DataFrame:
    """
    Convert quantity into integral form.

    Invalid values are flagged for quarantine instead of
    silently disappearing.
    """

    df = df.withColumn(
        "_quantity_numeric_text",
        _clean_numeric_text("quantity"),
    )

    df = df.withColumn(
        "quantity_int",
        F.expr(
            "try_cast(_quantity_numeric_text AS BIGINT)"
        ),
    )

    df = df.withColumn(
        "_dq_c3_quantity_invalid",
        (
            F.col("_quantity_numeric_text").isNotNull()
            & F.col("quantity_int").isNull()
        ),
    )

    return df.drop("_quantity_numeric_text")


def parse_invoice_date(
    df: DataFrame,
    date_formats: List[str],
) -> DataFrame:
    """
    C3-004 / C3-005 / C3-006

    Source InvoiceDate represents UK local time.

    Store:
      invoice_ts_local
      invoice_ts_utc
      invoice_timezone
      invoice_date_local
    """

    source = F.col("invoice_date").cast("string")

    timestamp_candidates = [
        F.to_timestamp(source, date_format)
        for date_format in date_formats
    ]

    df = df.withColumn(
        "invoice_ts_local",
        F.coalesce(*timestamp_candidates),
    )

    df = df.withColumn(
        "_dq_c3_invoice_date_invalid",
        (
            F.col("invoice_date").isNotNull()
            & F.col("invoice_ts_local").isNull()
        ),
    )

    # Source timestamps are interpreted as Europe/London local time.
    df = df.withColumn(
        "invoice_ts_utc",
        F.to_utc_timestamp(
            F.col("invoice_ts_local"),
            "Europe/London",
        ),
    )

    df = df.withColumn(
        "invoice_timezone",
        F.lit("Europe/London"),
    )

    df = df.withColumn(
        "invoice_date_local",
        F.to_date(
            F.col("invoice_ts_local")
        ),
    )

    return df


def preserve_invoice_as_text(df: DataFrame) -> DataFrame:
    """
    C3-007

    Invoice must stay textual so cancellation prefixes such
    as C123456 are never lost.
    """

    return df.withColumn(
        "invoice",
        F.col("invoice").cast("string"),
    )


def apply_type_coercion(
    df: DataFrame,
    date_formats: List[str],
) -> DataFrame:
    """
    Apply all C3 transformations.
    """

    df = preserve_invoice_as_text(df)

    df = coerce_price(df)

    df = coerce_quantity(df)

    df = parse_invoice_date(
        df,
        date_formats,
    )

    return df