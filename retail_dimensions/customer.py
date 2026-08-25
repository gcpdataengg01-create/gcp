"""Type 1 customer dimension transformations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    BooleanType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from retail_dimensions.keys import INVALID_KEY, UNKNOWN_KEY, deterministic_key


CUSTOMER_COLUMNS = [
    "customer_key",
    "customer_id",
    "country_code",
    "first_seen_date",
    "last_seen_date",
    "is_unknown",
    "is_invalid",
]


def _sentinel_schema() -> StructType:
    return StructType(
        [
            StructField("customer_key", LongType(), False),
            StructField("customer_id", StringType(), True),
            StructField("country_code", StringType(), True),
            StructField("first_seen_date", DateType(), True),
            StructField("last_seen_date", DateType(), True),
            StructField("is_unknown", BooleanType(), False),
            StructField("is_invalid", BooleanType(), False),
        ]
    )


def customer_sentinels(spark) -> DataFrame:
    """Return the required -1 Unknown and -2 Invalid dimension members."""

    return spark.createDataFrame(
        [
            (UNKNOWN_KEY, None, None, None, None, True, False),
            (INVALID_KEY, None, None, None, None, False, True),
        ],
        _sentinel_schema(),
    )


def customer_snapshot(clean_sales_df: DataFrame, business_date: str) -> DataFrame:
    """Create one deterministic customer record from one business-date batch.

    Missing customer IDs are not converted to normal dimension records; facts
    route them to customer_key=-1. For a customer seen multiple times in a
    batch, the latest transaction supplies the Type 1 attributes.
    """

    candidates = clean_sales_df.filter(F.col("customer_id").isNotNull())

    ordering = Window.partitionBy("customer_id").orderBy(
        F.col("invoice_ts_local").desc_nulls_last(),
        F.col("txn_id").desc_nulls_last(),
    )

    snapshot = (
        candidates.withColumn("_rn", F.row_number().over(ordering))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            deterministic_key("customer_id").alias("customer_key"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("country_code").cast("string").alias("country_code"),
            F.lit(business_date).cast("date").alias("first_seen_date"),
            F.lit(business_date).cast("date").alias("last_seen_date"),
            F.lit(False).alias("is_unknown"),
            F.lit(False).alias("is_invalid"),
        )
    )

    return snapshot


def merge_customer_type1(
    existing_df: DataFrame,
    today_df: DataFrame,
    business_date: str,
) -> DataFrame:
    """Apply deterministic Type 1 updates while retaining old customers."""

    existing_real = existing_df.filter(F.col("customer_key") > 0).select(
        *CUSTOMER_COLUMNS
    )

    today = today_df.select(*CUSTOMER_COLUMNS)

    old = existing_real.alias("old")
    new = today.alias("new")

    merged_existing = (
        old.join(
            new,
            on=F.col("old.customer_id") == F.col("new.customer_id"),
            how="left",
        )
        .select(
            F.col("old.customer_key").alias("customer_key"),
            F.col("old.customer_id").alias("customer_id"),
            F.when(
                F.col("new.customer_id").isNotNull(),
                F.col("new.country_code"),
            )
            .otherwise(F.col("old.country_code"))
            .alias("country_code"),
            F.col("old.first_seen_date").alias("first_seen_date"),
            F.when(
                F.col("new.customer_id").isNotNull(),
                F.lit(business_date).cast("date"),
            )
            .otherwise(F.col("old.last_seen_date"))
            .alias("last_seen_date"),
            F.lit(False).alias("is_unknown"),
            F.lit(False).alias("is_invalid"),
        )
    )

    new_customers = today.join(
        existing_real.select("customer_id"),
        on="customer_id",
        how="left_anti",
    )

    sentinels = existing_df.filter(F.col("customer_key").isin(UNKNOWN_KEY, INVALID_KEY)).select(
        *CUSTOMER_COLUMNS
    )

    return (
        sentinels.unionByName(merged_existing)
        .unionByName(new_customers)
        .dropDuplicates(["customer_key"])
    )
