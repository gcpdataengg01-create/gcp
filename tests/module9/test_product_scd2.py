from datetime import datetime

from pyspark.sql import functions as F

from retail_dimensions.scd2 import (
    apply_product_scd2,
    historical_product_join,
    product_sentinels,
    product_snapshot,
    validate_scd2_ranges,
)


def _sales(spark, rows):
    return spark.createDataFrame(
        rows,
        ["txn_id", "stock_code", "description", "invoice_ts_local"],
    )


def test_c8_product_change_closes_old_and_inserts_new(spark):
    day1_sales = _sales(
        spark,
        [(1, "A1", "Old description", datetime(2011, 3, 14, 9, 0))],
    )
    existing = product_sentinels(spark).unionByName(
        product_snapshot(day1_sales, "2011-03-14")
    )

    day2_sales = _sales(
        spark,
        [(2, "A1", "New description", datetime(2011, 3, 15, 9, 0))],
    )
    result = apply_product_scd2(
        existing,
        product_snapshot(day2_sales, "2011-03-15"),
        "2011-03-15",
    )

    validate_scd2_ranges(result)
    history = result.filter("stock_code = 'A1'").orderBy("valid_from").collect()

    assert len(history) == 2
    assert history[0]["description"] == "Old description"
    assert str(history[0]["valid_from"]) == "2011-03-14"
    assert str(history[0]["valid_to"]) == "2011-03-14"
    assert history[0]["is_current"] is False

    assert history[1]["description"] == "New description"
    assert str(history[1]["valid_from"]) == "2011-03-15"
    assert str(history[1]["valid_to"]) == "9999-12-31"
    assert history[1]["is_current"] is True
    assert history[0]["product_key"] != history[1]["product_key"]


def test_c8_rerun_is_idempotent(spark):
    sales = _sales(
        spark,
        [(1, "A1", "Description", datetime(2011, 3, 14, 9, 0))],
    )
    today = product_snapshot(sales, "2011-03-14")
    existing = product_sentinels(spark).unionByName(today)

    rerun = apply_product_scd2(existing, today, "2011-03-14")
    validate_scd2_ranges(rerun)

    assert rerun.filter("stock_code = 'A1'").count() == 1
    assert rerun.filter("stock_code = 'A1' AND is_current = true").count() == 1


def test_c8_same_day_correction_replaces_version(spark):
    original = _sales(
        spark,
        [(1, "A1", "Original", datetime(2011, 3, 14, 9, 0))],
    )
    existing = product_sentinels(spark).unionByName(
        product_snapshot(original, "2011-03-14")
    )

    corrected = _sales(
        spark,
        [(2, "A1", "Corrected", datetime(2011, 3, 14, 10, 0))],
    )
    result = apply_product_scd2(
        existing,
        product_snapshot(corrected, "2011-03-14"),
        "2011-03-14",
    )

    rows = result.filter("stock_code = 'A1'").collect()
    assert len(rows) == 1
    assert rows[0]["description"] == "Corrected"
    assert rows[0]["is_current"] is True


def test_c8_historical_join_uses_validity_range(spark):
    day1 = _sales(
        spark,
        [(1, "A1", "Old", datetime(2011, 3, 14, 9, 0))],
    )
    dim = product_sentinels(spark).unionByName(
        product_snapshot(day1, "2011-03-14")
    )

    day2 = _sales(
        spark,
        [(2, "A1", "New", datetime(2011, 3, 15, 9, 0))],
    )
    dim = apply_product_scd2(
        dim,
        product_snapshot(day2, "2011-03-15"),
        "2011-03-15",
    )

    facts = spark.createDataFrame(
        [("A1", "2011-03-14"), ("A1", "2011-03-15")],
        ["stock_code", "invoice_date_local"],
    ).withColumn("invoice_date_local", F.to_date("invoice_date_local"))

    joined = (
        historical_product_join(facts, dim)
        .select(F.col("f.invoice_date_local").alias("invoice_date_local"), F.col("p.description").alias("description"))
        .orderBy("invoice_date_local")
        .collect()
    )

    assert joined[0]["description"] == "Old"
    assert joined[1]["description"] == "New"
