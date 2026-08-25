-- T-05 post-build evidence: all fact dimension keys resolve, except the
-- intentionally reserved Unknown/Invalid keys -1 and -2.
SELECT
  COUNT(*) AS fact_rows,
  COUNTIF(p.product_key IS NULL AND f.product_key NOT IN (-1, -2)) AS product_orphans,
  COUNTIF(c.customer_key IS NULL AND f.customer_key NOT IN (-1, -2)) AS customer_orphans
FROM `PROJECT.curated.fct_sales_line` AS f
LEFT JOIN `PROJECT.curated.dim_product` AS p
  ON f.product_key = p.product_key
LEFT JOIN `PROJECT.curated.dim_customer` AS c
  ON f.customer_key = c.customer_key
WHERE f.invoice_date_local = @business_date;
