from typing import List, Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


SOURCE_SYSTEM = "retail_db"
ENTITY = "sales_txn"


QUARANTINE_RULES: List[Dict[str, str]] = [
    {
        "flag": "_dq_c2_required_field_missing",
        "rule_id": "C2-007",
        "severity": "QUARANTINE",
        "column_name": "required_business_field",
    },
    {
        "flag": "_dq_c3_price_invalid",
        "rule_id": "C3-003",
        "severity": "QUARANTINE",
        "column_name": "price",
    },
    {
        "flag": "_dq_c3_quantity_invalid",
        "rule_id": "C3-003",
        "severity": "QUARANTINE",
        "column_name": "quantity",
    },
    {
        "flag": "_dq_c3_invoice_date_invalid",
        "rule_id": "C3-004",
        "severity": "QUARANTINE",
        "column_name": "invoice_date",
    },
    {
        "flag": "_dq_c4_test_line",
        "rule_id": "C4-003",
        "severity": "QUARANTINE",
        "column_name": "stock_code",
    },
    {
        "flag": "_dq_c5_conflict",
        "rule_id": "C5-003",
        "severity": "QUARANTINE",
        "column_name": "invoice,stock_code",
    },
    {
        "flag": "_dq_c7_zero_quantity",
        "rule_id": "C7-003",
        "severity": "QUARANTINE",
        "column_name": "quantity",
    },
    {
        "flag": "_dq_c7_negative_price_invalid",
        "rule_id": "C7-005",
        "severity": "QUARANTINE",
        "column_name": "price",
    },
]


def add_required_field_flag(df: DataFrame) -> DataFrame:
    """
    C2-007

    Required business fields after normalisation:
      invoice
      stock_code
      quantity
      price
      invoice_date
    """

    return df.withColumn(
        "_dq_c2_required_field_missing",
        (
            F.col("invoice").isNull()
            | F.col("stock_code").isNull()
            | F.col("quantity").isNull()
            | F.col("price").isNull()
            | F.col("invoice_date").isNull()
        ),
    )


def _raw_payload_expression(df: DataFrame):
    """
    Preserve the complete row that entered the quarantine decision.
    """

    return F.to_json(
        F.struct(
            *[
                F.col(column_name)
                for column_name in df.columns
                if not column_name.startswith("_dq_")
            ]
        )
    )


def _offending_value(
    rule: Dict[str, str],
):
    column_name = rule["column_name"]

    if "," in column_name:
        columns = column_name.split(",")

        return F.concat_ws(
            "|",
            *[
                F.coalesce(
                    F.col(column).cast("string"),
                    F.lit("<NULL>"),
                )
                for column in columns
            ],
        )

    if column_name in {
        "required_business_field",
    }:
        return F.lit("one_or_more_required_fields_null")

    return F.coalesce(
        F.col(column_name).cast("string"),
        F.lit("<NULL>"),
    )


def build_quarantine_records(
    df: DataFrame,
    run_id: str,
    business_date: str,
) -> DataFrame:
    """
    Convert DQ flags into the required quarantine schema.

    A row can generate more than one quarantine record if it
    violates multiple independent rules.
    """

    raw_payload = _raw_payload_expression(df)

    outputs = []

    for rule in QUARANTINE_RULES:
        flag = rule["flag"]

        if flag not in df.columns:
            continue

        violation_df = (
            df.filter(
                F.coalesce(
                    F.col(flag),
                    F.lit(False),
                )
            )
            .select(
                F.sha2(
                    F.concat_ws(
                        "|",
                        F.lit(run_id),
                        F.coalesce(
                            F.col("txn_id").cast("string"),
                            F.lit("NO_TXN_ID"),
                        ),
                        F.lit(rule["rule_id"]),
                    ),
                    256,
                ).alias("quarantine_id"),

                F.lit(run_id).alias("run_id"),

                F.lit(business_date)
                .cast("date")
                .alias("business_date"),

                F.lit(SOURCE_SYSTEM)
                .alias("source_system"),

                F.lit(ENTITY)
                .alias("entity"),

                F.lit(rule["rule_id"])
                .alias("rule_id"),

                F.lit(rule["severity"])
                .alias("rule_severity"),

                F.lit(rule["column_name"])
                .alias("column_name"),

                _offending_value(rule)
                .alias("offending_value"),

                raw_payload.alias("raw_payload"),

                F.current_timestamp()
                .alias("detected_at"),

                F.lit(None)
                .cast("timestamp")
                .alias("reprocessed_at"),

                F.lit(None)
                .cast("string")
                .alias("reprocess_run_id"),
            )
        )

        outputs.append(violation_df)

    if not outputs:
        return None

    result = outputs[0]

    for output in outputs[1:]:
        result = result.unionByName(output)

    return result


def quarantine_condition(df: DataFrame):
    """
    True when a row violates at least one quarantine rule.
    """

    conditions = []

    for rule in QUARANTINE_RULES:
        if rule["flag"] in df.columns:
            conditions.append(
                F.coalesce(
                    F.col(rule["flag"]),
                    F.lit(False),
                )
            )

    if not conditions:
        return F.lit(False)

    combined = conditions[0]

    for condition in conditions[1:]:
        combined = combined | condition

    return combined


def split_valid_and_quarantine(
    df: DataFrame,
):
    """
    No silent drops.

    Every row either:
      - continues as valid
      - is represented in quarantine
    """

    condition = quarantine_condition(df)

    valid_df = df.filter(~condition)

    rejected_df = df.filter(condition)

    return valid_df, rejected_df


def write_quarantine(
    quarantine_df: DataFrame,
    quarantine_bucket: str,
    business_date: str,
    run_id: str,
    layer: str,
):
    """
    Write quarantine records into a deterministic layer-specific
    path.

    Different cleansing layers must never overwrite each other's
    quarantine records.
    """

    if quarantine_df is None:
        return

    path = (
        f"gs://{quarantine_bucket}/"
        f"entity={ENTITY}/"
        f"layer={layer}/"
        f"business_date={business_date}/"
        f"run_id={run_id}/"
    )

    (
        quarantine_df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("rule_id")
        .parquet(path)
    )