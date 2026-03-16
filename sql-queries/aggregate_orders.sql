CREATE TABLE IF NOT EXISTS processed.aggregated_orders AS
SELECT
  sku,
  SUM(quantity) AS total_demand,
  '{{EXEC_DATE}}' AS exec_date
FROM raw.orders
WHERE exec_date = '{{EXEC_DATE}}'
GROUP BY sku