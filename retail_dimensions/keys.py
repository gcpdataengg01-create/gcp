"""Deterministic surrogate-key helpers used by warehouse dimensions."""

from pyspark.sql import Column
from pyspark.sql import functions as F


UNKNOWN_KEY = -1
INVALID_KEY = -2

# Keep generated keys strictly positive so negative values remain reserved
# for warehouse sentinel members (-1 Unknown and -2 Invalid).
_MAX_GENERATED_KEY = 9_223_372_036_854_775_805


def deterministic_key(*column_names: str) -> Column:
    """Return a stable positive BIGINT key for the supplied business values.

    Spark's xxhash64 is deterministic for the same inputs. pmod keeps the
    generated value positive and therefore prevents collisions with the
    reserved negative sentinel keys.
    """

    if not column_names:
        raise ValueError("At least one column is required for a surrogate key")

    values = [
        F.coalesce(F.col(name).cast("string"), F.lit("~"))
        for name in column_names
    ]

    payload = F.concat_ws("||", *values)

    return (
        F.pmod(F.xxhash64(payload), F.lit(_MAX_GENERATED_KEY))
        + F.lit(1)
    ).cast("long")
