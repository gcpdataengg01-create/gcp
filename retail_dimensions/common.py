"""Common helpers for GCS-backed dimension jobs."""

from pyspark.sql import DataFrame, SparkSession


def path_exists(spark: SparkSession, path: str) -> bool:
    """Check whether a Hadoop-compatible path exists (including gs://)."""

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path_obj = jvm.org.apache.hadoop.fs.Path(path)
    fs = path_obj.getFileSystem(hadoop_conf)
    return bool(fs.exists(path_obj))


def materialize_for_overwrite(df: DataFrame) -> DataFrame:
    """Detach an output from source files that may be overwritten.

    Module 9 reads the current dimension state and writes the new state back
    to the same curated path. Materialising the result first prevents Spark
    from lazily re-reading files after overwrite has started.
    """

    materialized = df.cache()
    materialized.count()
    return materialized
