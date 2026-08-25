from decimal import Decimal

import pytest

from retail_fact.quality import require_pass, validate_c9_counts, validate_control_total


def test_c9_001_reconciles_rows():
    result = validate_c9_counts(
        rows_in=100,
        rows_out=95,
        quarantined_rows=3,
        deliberately_excluded_rows=2,
    )
    assert result["passed"] is True


def test_c9_001_detects_silent_loss():
    result = validate_c9_counts(
        rows_in=100,
        rows_out=94,
        quarantined_rows=3,
        deliberately_excluded_rows=2,
    )
    assert result["passed"] is False
    with pytest.raises(RuntimeError):
        require_pass(result)


def test_c9_002_accepts_one_cent_tolerance():
    result = validate_control_total(Decimal("100.00"), Decimal("100.01"))
    assert result["passed"] is True


def test_c9_002_rejects_more_than_one_cent():
    result = validate_control_total(Decimal("100.00"), Decimal("100.02"))
    assert result["passed"] is False
