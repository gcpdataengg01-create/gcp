"""SCD Type 2 transformations for dim_product (C8)."""

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


OPEN_END_DATE = "9999-12-31"
TRACKED_PRODUCT_COLUMNS = ["description"]

PRODUCT_COLUMNS = [
    "product_key",
    "stock_code",
    "description",
    "hash_diff",
    "valid_from",
    "valid_to",
    "is_current",
    "is_unknown",
    "is_invalid",
]


def _sentinel_schema() -> StructType:
    return StructType(
        [
            StructField("product_key", LongType(), False),
            StructField("stock_code", StringType(), True),
            StructField("description", StringType(), True),
            StructField("hash_diff", StringType(), True),
            StructField("valid_from", DateType(), True),
            StructField("valid_to", DateType(), True),
            StructField("is_current", BooleanType(), False),
            StructField("is_unknown", BooleanType(), False),
            StructField("is_invalid", BooleanType(), False),
        ]
    )


def product_sentinels(spark) -> DataFrame:
    """Return required -1 Unknown and -2 Invalid product members."""

    return spark.createDataFrame(
        [
            (UNKNOWN_KEY, None, "Unknown", None, None, None, True, True, False),
            (INVALID_KEY, None, "Invalid", None, None, None, True, False, True),
        ],
        _sentinel_schema(),
    )


def with_product_hash(df: DataFrame) -> DataFrame:
    """C8-002: hash tracked attributes only; exclude load metadata."""

    values = [
        F.coalesce(F.col(column_name).cast("string"), F.lit("~"))
        for column_name in TRACKED_PRODUCT_COLUMNS
    ]

    return df.withColumn(
        "hash_diff",
        F.sha2(F.concat_ws("||", *values), 256),
    )


def product_snapshot(clean_sales_df: DataFrame, business_date: str) -> DataFrame:
    """Return one deterministic product state per StockCode for the batch."""

    candidates = clean_sales_df.filter(F.col("stock_code").isNotNull())

    ordering = Window.partitionBy("stock_code").orderBy(
        F.col("invoice_ts_local").desc_nulls_last(),
        F.col("txn_id").desc_nulls_last(),
    )

    snapshot = (
        candidates.withColumn("_rn", F.row_number().over(ordering))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            F.col("stock_code").cast("string").alias("stock_code"),
            F.col("description").cast("string").alias("description"),
        )
    )

    snapshot = with_product_hash(snapshot)

    return (
        snapshot.withColumn("valid_from", F.lit(business_date).cast("date"))
        .withColumn("valid_to", F.lit(OPEN_END_DATE).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn("is_unknown", F.lit(False))
        .withColumn("is_invalid", F.lit(False))
        .withColumn(
            "product_key",
            deterministic_key("stock_code", "valid_from"),
        )
        .select(*PRODUCT_COLUMNS)
    )


def initial_product_dimension(today_df: DataFrame) -> DataFrame:
    """Create an initial real-product SCD2 dimension from a daily snapshot."""

    return today_df.select(*PRODUCT_COLUMNS)


def apply_product_scd2(
    existing_df: DataFrame,
    today_df: DataFrame,
    business_date: str,
) -> DataFrame:
    """Apply C8 SCD2 changes to the full existing product history.

    Rules:
    * unchanged products keep their existing current version;
    * new StockCodes receive a new current version;
    * changed products close the previous version on business_date - 1 and
      insert a new current version beginning on business_date;
    * a rerun correction on the SAME valid_from date replaces that version
      instead of creating an invalid zero/negative validity range;
    * products absent from today's batch remain current (absence is not a
      deletion signal).
    """

    existing = existing_df.select(*PRODUCT_COLUMNS)
    today = today_df.select(*PRODUCT_COLUMNS)

    sentinels = existing.filter(F.col("product_key").isin(UNKNOWN_KEY, INVALID_KEY))
    history_closed = existing.filter(
        (F.col("product_key") > 0) & (~F.col("is_current"))
    )
    current = existing.filter(
        (F.col("product_key") > 0) & F.col("is_current")
    )

    joined = current.alias("cur").join(
        today.alias("new"),
        on=F.col("cur.stock_code") == F.col("new.stock_code"),
        how="full_outer",
    )

    # Existing current versions that are unchanged or absent today remain open.
    unchanged_or_absent = joined.filter(
        F.col("cur.stock_code").isNotNull()
        & (
            F.col("new.stock_code").isNull()
            | (F.col("cur.hash_diff") == F.col("new.hash_diff"))
        )
    ).select(*[F.col(f"cur.{c}").alias(c) for c in PRODUCT_COLUMNS])

    changed = joined.filter(
        F.col("cur.stock_code").isNotNull()
        & F.col("new.stock_code").isNotNull()
        & (F.col("cur.hash_diff") != F.col("new.hash_diff"))
    )

    # Normal forward-time SCD2 change: close old row the day before new row.
    changed_forward = changed.filter(
        F.col("cur.valid_from") < F.lit(business_date).cast("date")
    )

    closed_old = changed_forward.select(
        F.col("cur.product_key").alias("product_key"),
        F.col("cur.stock_code").alias("stock_code"),
        F.col("cur.description").alias("description"),
        F.col("cur.hash_diff").alias("hash_diff"),
        F.col("cur.valid_from").alias("valid_from"),
        F.date_sub(F.lit(business_date).cast("date"), 1).alias("valid_to"),
        F.lit(False).alias("is_current"),
        F.lit(False).alias("is_unknown"),
        F.lit(False).alias("is_invalid"),
    )

    inserted_changed = changed_forward.select(
        *[F.col(f"new.{c}").alias(c) for c in PRODUCT_COLUMNS]
    )

    # Same-day rerun with corrected tracked attributes: replace current version.
    corrected_same_day = changed.filter(
        F.col("cur.valid_from") == F.lit(business_date).cast("date")
    ).select(*[F.col(f"new.{c}").alias(c) for c in PRODUCT_COLUMNS])

    # A batch older than the current version would require rebuilding history in
    # chronological order; silently mutating history would violate C8-004.
    backwards_change_count = changed.filter(
        F.col("cur.valid_from") > F.lit(business_date).cast("date")
    ).limit(1).count()
    if backwards_change_count:
        raise ValueError(
            "SCD2 batch is older than the current product version. "
            "Process backfill dates chronologically."
        )

    new_products = joined.filter(
        F.col("cur.stock_code").isNull()
        & F.col("new.stock_code").isNotNull()
    ).select(*[F.col(f"new.{c}").alias(c) for c in PRODUCT_COLUMNS])

    result = (
        sentinels.unionByName(history_closed)
        .unionByName(unchanged_or_absent)
        .unionByName(closed_old)
        .unionByName(inserted_changed)
        .unionByName(corrected_same_day)
        .unionByName(new_products)
        .dropDuplicates(["product_key"])
    )

    return result


def validate_scd2_ranges(df: DataFrame) -> None:
    """C8-003/C8-004: assert one current row and valid, gapless ranges."""

    real = df.filter(F.col("product_key") > 0)

    invalid_range = real.filter(F.col("valid_from") > F.col("valid_to")).limit(1).count()
    if invalid_range:
        raise ValueError("SCD2 contains valid_from > valid_to")

    duplicate_current = (
        real.filter(F.col("is_current"))
        .groupBy("stock_code")
        .count()
        .filter(F.col("count") != 1)
        .limit(1)
        .count()
    )
    if duplicate_current:
        raise ValueError("SCD2 must contain exactly one current row per StockCode")

    w = Window.partitionBy("stock_code").orderBy("valid_from", "valid_to")
    ranged = (
        real.withColumn("_prev_valid_to", F.lag("valid_to").over(w))
        .withColumn(
            "_expected_valid_from",
            F.date_add(F.col("_prev_valid_to"), 1),
        )
    )

    bad_contiguity = ranged.filter(
        F.col("_prev_valid_to").isNotNull()
        & (F.col("valid_from") != F.col("_expected_valid_from"))
    ).limit(1).count()
    if bad_contiguity:
        raise ValueError("SCD2 validity ranges are overlapping or non-contiguous")

    current_end_date_error = real.filter(
        F.col("is_current")
        & (F.col("valid_to") != F.lit(OPEN_END_DATE).cast("date"))
    ).limit(1).count()
    if current_end_date_error:
        raise ValueError("Current SCD2 rows must end on 9999-12-31")


def historical_product_join(
    fact_df: DataFrame,
    product_dim_df: DataFrame,
) -> DataFrame:
    """C8-005: join by StockCode and invoice date validity, never is_current."""

    products = product_dim_df.filter(F.col("product_key") > 0).alias("p")
    facts = fact_df.alias("f")

    condition = (
        (F.col("f.stock_code") == F.col("p.stock_code"))
        & (F.col("f.invoice_date_local") >= F.col("p.valid_from"))
        & (F.col("f.invoice_date_local") <= F.col("p.valid_to"))
    )

    return facts.join(products, condition, "left")
