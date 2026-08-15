import re
import unicodedata

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


@F.udf(returnType=StringType())
def normalize_unicode(value):
    if value is None:
        return None

    return unicodedata.normalize("NFC", value)


def normalise_string_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    C2-001 / C2-002 / C2-003

    - Replace NBSP and tabs
    - Strip control characters
    - Remove UTF-8 BOM
    - Collapse repeated whitespace
    - Trim leading/trailing whitespace
    - Unicode NFC normalization
    """

    col = F.col(column_name)

    cleaned = (
        F.regexp_replace(col, "\uFEFF", "")
    )

    cleaned = F.regexp_replace(
        cleaned,
        "\u00A0",
        " ",
    )

    cleaned = F.regexp_replace(
        cleaned,
        r"[\t\r\n]+",
        " ",
    )

    cleaned = F.regexp_replace(
        cleaned,
        r"[\x00-\x1F\x7F]",
        "",
    )

    cleaned = F.regexp_replace(
        cleaned,
        r"\s+",
        " ",
    )

    cleaned = F.trim(cleaned)

    return df.withColumn(
        column_name,
        normalize_unicode(cleaned),
    )


def map_sentinels_to_null(
    df: DataFrame,
    column_name: str,
    sentinel_values: list,
) -> DataFrame:
    """
    C2-004:
    Convert configured sentinel values to actual NULL.
    """

    normalized_sentinels = [
        str(value).strip().lower()
        for value in sentinel_values
    ]

    return df.withColumn(
        column_name,
        F.when(
            F.lower(
                F.trim(F.col(column_name).cast("string"))
            ).isin(normalized_sentinels),
            F.lit(None),
        ).otherwise(F.col(column_name)),
    )


def normalise_sales(
    df: DataFrame,
    sentinel_values: list,
) -> DataFrame:

    string_columns = [
        "invoice",
        "stock_code",
        "description",
        "customer_id",
        "country",
    ]

    existing_columns = set(df.columns)

    for column_name in string_columns:
        if column_name not in existing_columns:
            continue

        df = normalise_string_column(
            df,
            column_name,
        )

        df = map_sentinels_to_null(
            df,
            column_name,
            sentinel_values,
        )

    return df


def apply_operational_description_rules(
    df: DataFrame,
    operational_descriptions: list,
) -> DataFrame:
    """
    C2-005

    Configured operational Description notes:
      - description becomes NULL
      - is_adjustment becomes true

    Values are supplied from version-controlled configuration
    derived from source profiling.
    """

    normalized_values = [
        str(value).strip().upper()
        for value in operational_descriptions
    ]

    if not normalized_values:
        return df.withColumn(
            "is_adjustment",
            F.lit(False),
        )

    is_operational_note = (
        F.upper(
            F.trim(
                F.col("description")
            )
        ).isin(normalized_values)
    )

    return (
        df.withColumn(
            "is_adjustment",
            F.coalesce(
                is_operational_note,
                F.lit(False),
            ),
        )
        .withColumn(
            "description",
            F.when(
                F.col("is_adjustment"),
                F.lit(None),
            ).otherwise(
                F.col("description")
            ),
        )
    )