from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


SOURCE_PAYLOAD_COLUMNS = [
    "invoice",
    "stock_code",
    "description",
    "quantity_int",
    "invoice_ts_local",
    "price_decimal",
    "customer_id",
    "country",
]


def flag_business_key_conflicts(df: DataFrame) -> DataFrame:
    """
    C5-003

    A repeated (invoice, stock_code) where quantity or price
    differs is considered a conflicting duplicate.

    The assignment does not define a numeric tolerance, so
    normalized quantity/price values are compared exactly.
    """

    conflict_summary = (
        df.groupBy(
            "invoice",
            "stock_code",
        )
        .agg(
            F.countDistinct(
                F.struct(
                    F.col("quantity_int"),
                    F.col("price_decimal"),
                )
            ).alias("_business_value_variants")
        )
        .withColumn(
            "_dq_c5_conflict",
            F.col("_business_value_variants") > 1,
        )
    )

    return (
        df.join(
            conflict_summary,
            on=[
                "invoice",
                "stock_code",
            ],
            how="left",
        )
        .drop("_business_value_variants")
    )


def remove_exact_duplicates(df: DataFrame) -> DataFrame:
    """
    C5-001

    txn_id and ingestion metadata are deliberately excluded
    from duplicate comparison because they are unique for
    every extracted record.

    Keep the highest txn_id deterministically.
    """

    available_columns = [
        column
        for column in SOURCE_PAYLOAD_COLUMNS
        if column in df.columns
    ]

    window = (
        Window
        .partitionBy(*available_columns)
        .orderBy(
            F.col("txn_id").desc()
        )
    )

    return (
        df.withColumn(
            "_exact_duplicate_rank",
            F.row_number().over(window),
        )
        .withColumn(
            "_dq_c5_exact_duplicate",
            F.col("_exact_duplicate_rank") > 1,
        )
    )


def keep_non_exact_duplicates(
    df: DataFrame,
) -> DataFrame:
    """
    C5-001 repair step.
    """

    return (
        df.filter(
            F.col("_dq_c5_exact_duplicate") == F.lit(False)
        )
        .drop("_exact_duplicate_rank")
    )


def dedupe_business_key(
    df: DataFrame,
) -> DataFrame:
    """
    C5-002

    Deterministic business-key dedupe.

    Keep:
        latest invoice timestamp
        then highest txn_id
    """

    window = (
        Window
        .partitionBy(
            "invoice",
            "stock_code",
        )
        .orderBy(
            F.col("invoice_ts_local").desc(),
            F.col("txn_id").desc(),
        )
    )

    return (
        df.withColumn(
            "_business_key_rank",
            F.row_number().over(window),
        )
        .withColumn(
            "_dq_c5_business_duplicate",
            F.col("_business_key_rank") > 1,
        )
    )


def keep_business_key_winner(
    df: DataFrame,
) -> DataFrame:
    return (
        df.filter(
            F.col("_business_key_rank") == 1
        )
        .drop("_business_key_rank")
    )


def dedupe_against_target(
    incoming_df: DataFrame,
    target_df: DataFrame,
    key_columns: List[str],
) -> DataFrame:
    """
    C5-004

    Remove records whose idempotency/business key already
    exists in current target state.

    The actual target key will be supplied by the downstream
    fact/publish implementation.
    """

    target_keys = (
        target_df
        .select(*key_columns)
        .dropDuplicates()
    )

    return incoming_df.join(
        target_keys,
        on=key_columns,
        how="left_anti",
    )