from jobs.dimensions.build_date import build_date_dimension


def test_dim_date_required_range(spark):
    df = build_date_dimension(spark, "2009-01-01", "2012-12-31")

    assert df.count() == 1461
    first = df.orderBy("calendar_date").first()
    last = df.orderBy("calendar_date", ascending=False).first()

    assert str(first["calendar_date"]) == "2009-01-01"
    assert first["date_key"] == 20090101
    assert str(last["calendar_date"]) == "2012-12-31"
    assert last["date_key"] == 20121231


def test_dim_date_weekend_and_business_day(spark):
    df = build_date_dimension(spark, "2011-03-11", "2011-03-12")
    rows = {str(row["calendar_date"]): row for row in df.collect()}

    assert rows["2011-03-11"]["is_business_day"] is True   # Friday
    assert rows["2011-03-12"]["is_business_day"] is False  # Saturday
