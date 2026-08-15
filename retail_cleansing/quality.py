from enum import Enum
from typing import Optional

from pyspark.sql import DataFrame


class QualityAction(str, Enum):
    FAIL_JOB = "FAIL_JOB"
    QUARANTINE = "QUARANTINE"
    REPAIR = "REPAIR"
    ROUTE_UNKNOWN = "ROUTE_UNKNOWN"
    PASS_WITH_FLAG = "PASS_WITH_FLAG"


EXPECTED_SOURCE_COLUMNS = [
    "txn_id",
    "invoice",
    "stock_code",
    "description",
    "quantity",
    "invoice_date",
    "price",
    "customer_id",
    "country",
]


REQUIRED_BUSINESS_COLUMNS = [
    "invoice",
    "stock_code",
    "quantity",
    "price",
    "invoice_date",
]


class DataQualityFailure(RuntimeError):
    pass


def validate_non_empty(
    df: DataFrame,
    allow_non_trading_day: bool = False,
) -> int:
    """
    C1-001

    Extract must return rows unless dim_date confirms that
    the requested date is a legitimate non-trading date.

    dim_date integration happens when the date dimension is
    available; the caller passes allow_non_trading_day.
    """

    row_count = df.count()

    if row_count == 0 and not allow_non_trading_day:
        raise DataQualityFailure(
            "C1-001 FAIL_JOB: extract returned zero rows."
        )

    return row_count


def validate_expected_columns(
    df: DataFrame,
):
    """
    C1-003

    Required source columns must exist and be correctly ordered.
    Ingestion metadata may appear after source columns.
    """

    actual_source_columns = [
        column
        for column in df.columns
        if column in EXPECTED_SOURCE_COLUMNS
    ]

    if actual_source_columns != EXPECTED_SOURCE_COLUMNS:
        raise DataQualityFailure(
            "C1-003 FAIL_JOB: unexpected source schema. "
            f"expected={EXPECTED_SOURCE_COLUMNS}, "
            f"actual={actual_source_columns}"
        )


def validate_price_schema(
    df: DataFrame,
):
    """
    C3-002

    Price must be represented as decimal(18,4).
    """

    field = next(
        (
            field
            for field in df.schema.fields
            if field.name == "price_decimal"
        ),
        None,
    )

    if field is None:
        raise DataQualityFailure(
            "C3-002 FAIL_JOB: price_decimal is missing."
        )

    if field.dataType.simpleString() != "decimal(18,4)":
        raise DataQualityFailure(
            "C3-002 FAIL_JOB: price_decimal must be "
            f"decimal(18,4), got {field.dataType.simpleString()}."
        )


def validate_row_count_range(
    current_count: int,
    trailing_same_weekday_average: Optional[float],
):
    """
    C1-002

    Current count should stay within 30%-300% of the trailing
    14-day same-weekday average.

    This is an ALERT/HOLD rule, not a silent correction.
    """

    if trailing_same_weekday_average is None:
        return {
            "rule_id": "C1-002",
            "status": "NOT_EVALUATED",
            "reason": "No trailing same-weekday baseline supplied",
        }

    lower = trailing_same_weekday_average * 0.30
    upper = trailing_same_weekday_average * 3.00

    passed = lower <= current_count <= upper

    return {
        "rule_id": "C1-002",
        "status": "PASS" if passed else "HOLD",
        "current_count": current_count,
        "baseline": trailing_same_weekday_average,
        "lower_bound": lower,
        "upper_bound": upper,
    }


def validate_fx_presence(
    df: DataFrame,
):
    """
    C7-009

    Missing FX data is a FAIL_JOB condition.
    """

    if "_dq_c7_fx_missing" not in df.columns:
        raise DataQualityFailure(
            "C7-009 FAIL_JOB: FX validation column missing."
        )

    missing_count = (
        df.filter("_dq_c7_fx_missing = true")
        .limit(1)
        .count()
    )

    if missing_count > 0:
        raise DataQualityFailure(
            "C7-009 FAIL_JOB: one or more sales rows "
            "do not have required GBP/EUR FX reference data."
        )


def calculate_quarantine_rate(
    extracted_rows: int,
    quarantined_source_rows: int,
) -> float:
    if extracted_rows == 0:
        return 0.0

    return quarantined_source_rows / extracted_rows


def evaluate_quarantine_threshold(
    extracted_rows: int,
    quarantined_source_rows: int,
):
    """
    SPEC 4.9 / C9 input.

    The final publish decision belongs to Module 10, but
    Module 8 calculates the metric needed by that gate.
    """

    rate = calculate_quarantine_rate(
        extracted_rows,
        quarantined_source_rows,
    )

    return {
        "extracted_rows": extracted_rows,
        "quarantined_rows": quarantined_source_rows,
        "quarantine_rate": rate,
        "threshold": 0.02,
        "threshold_exceeded": rate > 0.02,
    }