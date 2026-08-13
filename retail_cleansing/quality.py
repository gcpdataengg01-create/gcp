from enum import Enum


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