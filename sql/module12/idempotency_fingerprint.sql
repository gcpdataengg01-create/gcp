-- T-02: run before and after rerunning the same business date.
-- Acceptance: identical row_count, control_total_gbp and sorted_output_sha256.
WITH scoped AS (
  SELECT *
  FROM `PROJECT.curated.fct_sales_line`
  WHERE invoice_date_local = @business_date
), ordered AS (
  SELECT TO_JSON_STRING(t) AS row_json
  FROM scoped AS t
  ORDER BY invoice, stock_code, txn_id
)
SELECT
  (SELECT COUNT(*) FROM scoped) AS row_count,
  (SELECT ROUND(COALESCE(SUM(line_amount_gbp), 0), 2) FROM scoped) AS control_total_gbp,
  TO_HEX(SHA256(STRING_AGG(row_json, '\n'))) AS sorted_output_sha256
FROM ordered;
