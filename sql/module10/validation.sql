-- C9-003 duplicate check
SELECT invoice, stock_code, COUNT(*) AS cnt
FROM `PROJECT.curated.fct_sales_line`
WHERE invoice_date_local = @business_date
GROUP BY invoice, stock_code
HAVING COUNT(*) > 1;

-- C9-004 product orphan check
SELECT COUNT(*) AS orphan_count
FROM `PROJECT.curated.fct_sales_line` f
LEFT JOIN `PROJECT.curated.dim_product` p
  ON f.product_key = p.product_key
WHERE f.invoice_date_local = @business_date
  AND p.product_key IS NULL
  AND f.product_key NOT IN (-1, -2);

-- C9-005 batch-date check
SELECT MAX(invoice_date_local) AS newest_invoice_date
FROM `PROJECT.curated.fct_sales_line`
WHERE invoice_date_local = @business_date;
