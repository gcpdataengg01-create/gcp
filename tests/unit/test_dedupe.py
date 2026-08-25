from datetime import datetime
from decimal import Decimal

from retail_cleansing.dedupe import (
    remove_exact_duplicates,
    keep_non_exact_duplicates,
    flag_business_key_conflicts,
    dedupe_business_key,
    keep_business_key_winner,
)


def test_c5_exact_duplicate_removal(spark):
    df = spark.createDataFrame(
        [
            (
                1,
                "100",
                "A",
                2,
                Decimal("4.0000"),
            ),
            (
                2,
                "100",
                "A",
                2,
                Decimal("4.0000"),
            ),
        ],
        [
            "txn_id",
            "invoice",
            "stock_code",
            "quantity_int",
            "price_decimal",
        ],
    )

    flagged = remove_exact_duplicates(df)

    duplicate_count = (
        flagged
        .filter("_dq_c5_exact_duplicate = true")
        .count()
    )

    assert duplicate_count == 1

    result = keep_non_exact_duplicates(
        flagged
    )

    assert result.count() == 1

    # Highest txn_id wins deterministically.
    assert result.first()["txn_id"] == 2


def test_c5_conflicting_duplicate_is_flagged(spark):
    df = spark.createDataFrame(
        [
            (
                1,
                "100",
                "A",
                2,
                Decimal("4.0000"),
            ),
            (
                2,
                "100",
                "A",
                5,
                Decimal("4.0000"),
            ),
        ],
        [
            "txn_id",
            "invoice",
            "stock_code",
            "quantity_int",
            "price_decimal",
        ],
    )

    result = flag_business_key_conflicts(df)

    assert (
        result
        .filter("_dq_c5_conflict = true")
        .count()
        == 2
    )


def test_c5_business_key_winner_is_deterministic(spark):
    df = spark.createDataFrame(
        [
            (
                10,
                "100",
                "A",
                datetime(
                    2011,
                    3,
                    14,
                    10,
                    0,
                ),
            ),
            (
                20,
                "100",
                "A",
                datetime(
                    2011,
                    3,
                    14,
                    11,
                    0,
                ),
            ),
        ],
        [
            "txn_id",
            "invoice",
            "stock_code",
            "invoice_ts_local",
        ],
    )

    ranked = dedupe_business_key(df)

    result = keep_business_key_winner(
        ranked
    )

    assert result.count() == 1

    assert result.first()["txn_id"] == 20