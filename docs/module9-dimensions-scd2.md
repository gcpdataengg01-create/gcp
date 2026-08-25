# Module 9 - Dimensions and SCD Type 2 (C8)

## Scope

Module 9 builds the three warehouse dimensions in Spark:

- `dim_date`: 2009-01-01 through 2012-12-31, including `is_business_day`.
- `dim_customer`: Type 1 by `customer_id`, with `-1` Unknown and `-2` Invalid members.
- `dim_product`: SCD Type 2 by `stock_code`, tracking `description`, with `-1` Unknown and `-2` Invalid members.

The input for customer and product jobs is the Module 8 clean stage path:

`gs://<stage-bucket>/entity=sales_clean/business_date=<YYYY-MM-DD>/`

The dimensions are written as Snappy Parquet under the curated bucket.

## C8 implementation

- **C8-001**: surrogate keys are deterministic positive BIGINT values; negative keys remain reserved for sentinel members.
- **C8-002**: `dim_product.hash_diff` is SHA-256 over the tracked `description`; operational/load metadata is excluded.
- **C8-003**: a changed product closes the current version on the day before the new version and inserts a new current version with `valid_to=9999-12-31`.
- **C8-004**: validation rejects invalid, overlapping, or non-contiguous product validity ranges and enforces exactly one current row per real StockCode.
- **C8-005**: historical facts join on `stock_code` plus `invoice_date_local` within `[valid_from, valid_to]`; the join does not filter to `is_current=true`.

The SCD2 job expects business dates to be processed chronologically. A same-day rerun is idempotent. A same-day source correction replaces that day's current version instead of producing an invalid SCD range.

## Business-day assumption

The assignment requires `is_business_day` but does not supply a governed holiday calendar. `dim_date` therefore uses Monday-Friday as the initial business-day definition. A holiday reference can be incorporated later without changing the date-dimension grain.

## Local validation

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\module9\validate_module9.ps1
python -m pytest .\tests\module9 -v
powershell -ExecutionPolicy Bypass -File .\scripts\package\build_dimensions_package.ps1
```

Local Spark execution can be deferred to the final GCP validation cycle; syntax checks should still be run before commit.

## Dataproc packaging

Upload `dist/retail_dimensions.zip` to the code/dependency bucket and supply it through `--py-files` when submitting the customer or product jobs.

Representative arguments:

```text
build_customer.py --business-date=2011-03-14 --stage-bucket=<stage> --curated-bucket=<curated>
product_scd2.py --business-date=2011-03-14 --stage-bucket=<stage> --curated-bucket=<curated>
build_date.py --curated-bucket=<curated>
```
