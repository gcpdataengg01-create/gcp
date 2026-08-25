"""C9 publish-gate validation helpers."""

from decimal import Decimal
from typing import Any, Dict


CONTROL_TOTAL_TOLERANCE = Decimal("0.01")


def validate_c9_counts(
    rows_in: int,
    rows_out: int,
    quarantined_rows: int,
    deliberately_excluded_rows: int,
) -> Dict[str, Any]:
    """C9-001: no silent loss across the business-date pipeline."""

    expected = rows_out + quarantined_rows + deliberately_excluded_rows
    passed = rows_in == expected
    return {
        "rule_id": "C9-001",
        "passed": passed,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "quarantined_rows": quarantined_rows,
        "deliberately_excluded_rows": deliberately_excluded_rows,
        "reconciled_rows": expected,
    }


def validate_control_total(
    source_total,
    target_total,
    tolerance: Decimal = CONTROL_TOTAL_TOLERANCE,
) -> Dict[str, Any]:
    """C9-002: source and target GBP totals must match within +/-0.01."""

    source = Decimal(str(source_total or 0)).quantize(Decimal("0.01"))
    target = Decimal(str(target_total or 0)).quantize(Decimal("0.01"))
    variance = (target - source).quantize(Decimal("0.01"))
    passed = abs(variance) <= tolerance
    return {
        "rule_id": "C9-002",
        "passed": passed,
        "source_total": str(source),
        "target_total": str(target),
        "variance": str(variance),
        "tolerance": str(tolerance),
    }


def require_pass(result: Dict[str, Any]) -> None:
    if not result.get("passed"):
        raise RuntimeError(f"{result.get('rule_id')} publish gate failed: {result}")
