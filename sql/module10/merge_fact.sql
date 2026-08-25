-- Module 10 reference MERGE. The runtime Python job substitutes PROJECT and
-- the run-scoped staging table and binds @business_date.
MERGE `PROJECT.curated.fct_sales_line` AS tgt
USING `PROJECT.staging.RUN_SCOPED_STAGE_TABLE` AS src
ON tgt.invoice = src.invoice
 AND tgt.stock_code = src.stock_code
 AND tgt.invoice_date_local = @business_date
WHEN MATCHED THEN UPDATE SET
  source_txn_id = src.source_txn_id,
  date_key = src.date_key,
  customer_key = src.customer_key,
  product_key = src.product_key,
  customer_id = src.customer_id,
  country_code = src.country_code,
  quantity = src.quantity,
  price = src.price,
  line_amount_gbp = src.line_amount_gbp,
  line_amount_eur = src.line_amount_eur,
  fx_rate = src.fx_rate,
  fx_rate_date = src.fx_rate_date,
  fx_rate_is_carried = src.fx_rate_is_carried,
  currency = src.currency,
  line_type = src.line_type,
  is_cancellation = src.is_cancellation,
  is_return_adjustment = src.is_return_adjustment,
  is_free_item = src.is_free_item,
  is_extreme_outlier = src.is_extreme_outlier,
  run_id = src.run_id,
  loaded_at = src.loaded_at
WHEN NOT MATCHED THEN INSERT ROW
WHEN NOT MATCHED BY SOURCE
 AND tgt.invoice_date_local = @business_date THEN DELETE;
