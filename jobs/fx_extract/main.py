"""Module 7 GBP->EUR FX reference extractor for Cloud Run Jobs."""

import io
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.cloud import storage

API_URL = "https://api.frankfurter.dev/v2/rates"


def _fetch_rate(requested: date, base: str, quote: str):
    """Fetch the requested date, carrying the most recent prior published rate."""
    session = requests.Session()
    for offset in range(0, 8):
        candidate = requested - timedelta(days=offset)
        response = session.get(
            API_URL,
            params={"date": candidate.isoformat(), "base": base, "symbols": quote},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()

        row = None
        if isinstance(payload, list) and payload:
            row = payload[0]
        elif isinstance(payload, dict):
            if "rate" in payload:
                row = payload
            elif "rates" in payload and quote in payload["rates"]:
                row = {
                    "date": payload.get("date", candidate.isoformat()),
                    "base": payload.get("base", base),
                    "quote": quote,
                    "rate": payload["rates"][quote],
                }

        if row and row.get("rate") is not None:
            return candidate, Decimal(str(row["rate"]))

    raise RuntimeError(f"No {base}->{quote} FX rate found on/before {requested}")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    project_id = os.environ["PROJECT_ID"]
    bucket_name = os.environ["RAW_BUCKET"]
    base = os.getenv("FX_BASE", "GBP")
    quote = os.getenv("FX_QUOTE", "EUR")
    requested_text = os.getenv("BUSINESS_DATE")
    run_id = os.getenv("RUN_ID")

    # Cloud Scheduler invokes the reference-cache job independently of Composer.
    # Composer always overrides BUSINESS_DATE explicitly for pipeline/backfill runs.
    if requested_text:
        requested = datetime.strptime(requested_text, "%Y-%m-%d").date()
    else:
        requested = datetime.now(timezone.utc).date()

    if not run_id:
        run_id = datetime.now(timezone.utc).strftime("scheduler-%Y%m%dT%H%M%SZ")

    rate_date, rate = _fetch_rate(requested, base, quote)
    carried = rate_date != requested

    schema = pa.schema(
        [
            ("requested_date", pa.date32()),
            ("fx_rate_date", pa.date32()),
            ("fx_rate", pa.decimal128(18, 8)),
            ("fx_rate_is_carried", pa.bool_()),
            ("base", pa.string()),
            ("quote", pa.string()),
        ]
    )
    table = pa.Table.from_pylist(
        [
            {
                "requested_date": requested,
                "fx_rate_date": rate_date,
                "fx_rate": rate.quantize(Decimal("0.00000001")),
                "fx_rate_is_carried": carried,
                "base": base,
                "quote": quote,
            }
        ],
        schema=schema,
    )

    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    safe_run = re.sub(r"[^A-Za-z0-9_.-]", "-", run_id)[:120]
    object_name = (
        f"reference/fx/requested_date={requested.isoformat()}/"
        f"rate-{safe_run}.parquet"
    )
    client = storage.Client(project=project_id)
    blob = client.bucket(bucket_name).blob(object_name)
    blob.upload_from_file(buffer, content_type="application/octet-stream", if_generation_match=0)

    logging.info(
        "MODULE7_FX_METRIC %s",
        json.dumps(
            {
                "requested_date": requested.isoformat(),
                "fx_rate_date": rate_date.isoformat(),
                "fx_rate": str(rate),
                "fx_rate_is_carried": carried,
                "base": base,
                "quote": quote,
                "gcs_uri": f"gs://{bucket_name}/{object_name}",
            }
        ),
    )


if __name__ == "__main__":
    main()
