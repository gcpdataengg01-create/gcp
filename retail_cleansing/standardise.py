from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _mapping_expression(mapping: Dict[str, str]):
    """
    Build a Spark map expression from reference configuration.
    """

    if not mapping:
        return None

    entries = []

    for key, value in mapping.items():
        entries.extend(
            [
                F.lit(str(key).strip().upper()),
                F.lit(value),
            ]
        )

    return F.create_map(*entries)


def standardise_country(
    df: DataFrame,
    country_map: Dict[str, str],
) -> DataFrame:
    """
    C4-001

    Map source country names to ISO-3166 alpha-2.

    Unmapped countries remain in the dataset and are routed
    to Unknown later.
    """

    normalized_country = F.upper(
        F.trim(F.col("country"))
    )

    country_mapping = {
        str(key).strip().upper(): value
        for key, value in country_map.items()
    }

    country_expr = _mapping_expression(
        country_mapping
    )

    if country_expr is None:
        df = df.withColumn(
            "country_code",
            F.lit(None).cast("string"),
        )
    else:
        df = df.withColumn(
            "country_code",
            country_expr.getItem(
                normalized_country
            ),
        )

    df = df.withColumn(
        "_dq_c4_country_unknown",
        (
            F.col("country").isNotNull()
            & F.col("country_code").isNull()
        ),
    )

    return df


def classify_stock_code(
    df: DataFrame,
    stock_code_types: Dict[str, dict],
) -> DataFrame:
    """
    C4-002 / C4-003 / C4-004

    Classify non-product StockCodes using reference data.

    Default:
        product

    Examples:
        POST         -> shipping
        D            -> discount
        BANK CHARGES -> fee
        M            -> adjustment
        S            -> sample
        TEST001      -> test
    """

    mapping = {}

    for stock_code, config in stock_code_types.items():
        mapping[
            str(stock_code).strip().upper()
        ] = config["line_type"]

    stock_code_expr = _mapping_expression(
        mapping
    )

    normalized_stock_code = F.upper(
        F.trim(F.col("stock_code"))
    )

    if stock_code_expr is None:
        df = df.withColumn(
            "line_type",
            F.lit("product"),
        )
    else:
        df = df.withColumn(
            "line_type",
            F.coalesce(
                stock_code_expr.getItem(
                    normalized_stock_code
                ),
                F.lit("product"),
            ),
        )

    # Test records must not enter the published fact.
    df = df.withColumn(
        "_dq_c4_test_line",
        F.col("line_type") == F.lit("test"),
    )

    return df


def set_currency(df: DataFrame) -> DataFrame:
    """
    C4-005

    Online Retail II source transactions are GBP.
    """

    return df.withColumn(
        "currency",
        F.lit("GBP"),
    )


def apply_domain_standardisation(
    df: DataFrame,
    country_map: Dict[str, str],
    stock_code_types: Dict[str, dict],
) -> DataFrame:
    """
    Apply C4 standardisation.
    """

    df = standardise_country(
        df,
        country_map,
    )

    df = classify_stock_code(
        df,
        stock_code_types,
    )

    df = set_currency(df)

    return df