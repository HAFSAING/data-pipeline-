CREATE SCHEMA IF NOT EXISTS hive.raw;

DROP TABLE IF EXISTS hive.raw.orders_raw;
CREATE TABLE hive.raw.orders_raw (
  order_id VARCHAR,
  store_id VARCHAR,
  sku VARCHAR,
  quantity INTEGER,
  timestamp VARCHAR
)
WITH (
  format = 'JSON',
  external_location = 'hdfs://namenode:9000/raw/orders/{{EXEC_DATE}}'
);

-- Aggregation
DROP TABLE IF EXISTS hive.processed.aggregated_orders;
CREATE TABLE hive.processed.aggregated_orders AS
SELECT
  sku,
  SUM(quantity) AS total_demand
FROM hive.raw.orders_raw
GROUP BY sku;