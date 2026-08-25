-- Module 11 reference only.
-- Terraform creates equivalent Dataplex sql_assertion rules.
-- `${data()}` is a Dataplex DataScan source-table placeholder, not normal BigQuery SQL.

-- C9-003: no duplicate business-key groups.
SELECT invoice, stock_code
FROM ${data()}
GROUP BY invoice, stock_code
HAVING COUNT(*) > 1;

-- C9-004: no product/customer orphans except -1/-2 reserved members.
SELECT f.product_key, f.customer_key
FROM ${data()} AS f
LEFT JOIN `PROJECT.curated.dim_product` AS p
  ON f.product_key = p.product_key
LEFT JOIN `PROJECT.curated.dim_customer` AS c
  ON f.customer_key = c.customer_key
WHERE (p.product_key IS NULL AND f.product_key NOT IN (-1, -2))
   OR (c.customer_key IS NULL AND f.customer_key NOT IN (-1, -2));
