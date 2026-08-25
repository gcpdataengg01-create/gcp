from datetime import date, datetime

from retail_dimensions.customer import (
    customer_sentinels,
    customer_snapshot,
    merge_customer_type1,
)


def _sales(spark, rows):
    return spark.createDataFrame(
        rows,
        ["txn_id", "customer_id", "country_code", "invoice_ts_local"],
    )


def test_customer_snapshot_has_stable_key_and_ignores_missing_customer(spark):
    sales = _sales(
        spark,
        [
            (1, "123", "GB", datetime(2011, 3, 14, 9, 0)),
            (2, "123", "FR", datetime(2011, 3, 14, 10, 0)),
            (3, None, "GB", datetime(2011, 3, 14, 11, 0)),
        ],
    )

    result = customer_snapshot(sales, "2011-03-14")
    assert result.count() == 1
    row = result.first()
    assert row["customer_key"] > 0
    assert row["customer_id"] == "123"
    assert row["country_code"] == "FR"


def test_customer_type1_updates_attribute_without_changing_key(spark):
    day1_sales = _sales(
        spark,
        [(1, "123", "GB", datetime(2011, 3, 14, 9, 0))],
    )
    day1 = customer_sentinels(spark).unionByName(
        customer_snapshot(day1_sales, "2011-03-14")
    )
    old = day1.filter("customer_id = '123'").first()

    day2_sales = _sales(
        spark,
        [(2, "123", "FR", datetime(2011, 3, 15, 9, 0))],
    )
    merged = merge_customer_type1(
        day1,
        customer_snapshot(day2_sales, "2011-03-15"),
        "2011-03-15",
    )
    new = merged.filter("customer_id = '123'").first()

    assert new["customer_key"] == old["customer_key"]
    assert new["country_code"] == "FR"
    assert new["first_seen_date"] == date(2011, 3, 14)
    assert new["last_seen_date"] == date(2011, 3, 15)
    assert merged.filter("customer_key = -1").count() == 1
    assert merged.filter("customer_key = -2").count() == 1
