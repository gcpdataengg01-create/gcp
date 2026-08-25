import pytest


@pytest.fixture(scope="session")
def spark():
    # Import lazily so pure-Python Module 10 quality tests can run even on a
    # workstation where Spark execution is intentionally deferred to GCP.
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("retail-etl-module10-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
