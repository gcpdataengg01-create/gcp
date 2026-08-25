-- T-03 warehouse side. Compare this result with the source PostgreSQL control
-- total captured in Firestore/ops.etl_batch_control. Variance must be <= 0.01.
SELECT
  business_date,
  source_control_total,
  target_control_total,
  control_total_variance,
  ABS(control_total_variance) <= NUMERIC '0.01' AS passed
FROM `PROJECT.ops.etl_batch_control`
WHERE business_date = @business_date
ORDER BY published_at DESC
LIMIT 1;
