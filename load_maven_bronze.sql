-- Maven Analytics Toy Store bronze ingestion for DuckDB
-- Run from repository root:
--   duckdb mff_analytics.duckdb < load_maven_bronze.sql
--
-- Update these paths if your local layout differs.
-- The script is portable by default when run from repository root.

CREATE SCHEMA IF NOT EXISTS raw;

-- 1) Read source CSVs into staging tables
CREATE OR REPLACE TABLE raw.stg_orders AS
SELECT * FROM read_csv_auto('data/raw/maven_toy_store/orders.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE raw.stg_order_items AS
SELECT * FROM read_csv_auto('data/raw/maven_toy_store/order_items.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE raw.stg_order_item_refunds AS
SELECT * FROM read_csv_auto('data/raw/maven_toy_store/order_item_refunds.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE raw.stg_website_sessions AS
SELECT * FROM read_csv_auto('data/raw/maven_toy_store/website_sessions.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE raw.stg_website_pageviews AS
SELECT * FROM read_csv_auto('data/raw/maven_toy_store/website_pageviews.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE raw.stg_products AS
SELECT * FROM read_csv_auto('data/raw/maven_toy_store/products.csv', HEADER = TRUE);

-- Optional metadata file from the download bundle
CREATE OR REPLACE TABLE raw.stg_mff_data_dictionary AS
SELECT * FROM read_csv_auto('data/raw/maven_toy_store/maven_fuzzy_factory_data_dictionary.csv', HEADER = TRUE);

-- 2) Create standardized raw tables
CREATE OR REPLACE TABLE raw.orders AS
SELECT
  *,
  year(CAST(created_at AS TIMESTAMP)) AS year,
  month(CAST(created_at AS TIMESTAMP)) AS month
FROM raw.stg_orders;

CREATE OR REPLACE TABLE raw.order_items AS
SELECT
  *,
  year(CAST(created_at AS TIMESTAMP)) AS year,
  month(CAST(created_at AS TIMESTAMP)) AS month
FROM raw.stg_order_items;

CREATE OR REPLACE TABLE raw.order_item_refunds AS
SELECT
  *,
  year(CAST(created_at AS TIMESTAMP)) AS year,
  month(CAST(created_at AS TIMESTAMP)) AS month
FROM raw.stg_order_item_refunds;

CREATE OR REPLACE TABLE raw.website_sessions AS
SELECT
  *,
  year(CAST(created_at AS TIMESTAMP)) AS year,
  month(CAST(created_at AS TIMESTAMP)) AS month
FROM raw.stg_website_sessions;

CREATE OR REPLACE TABLE raw.website_pageviews AS
SELECT
  *,
  year(CAST(created_at AS TIMESTAMP)) AS year,
  month(CAST(created_at AS TIMESTAMP)) AS month
FROM raw.stg_website_pageviews;

-- Product attributes are usually static; keep as non-partitioned raw table
CREATE OR REPLACE TABLE raw.products AS
SELECT * FROM raw.stg_products;

CREATE OR REPLACE TABLE raw.mff_data_dictionary AS
SELECT * FROM raw.stg_mff_data_dictionary;

-- 3) Export to partitioned Parquet datasets for dbt external sources
COPY (
  SELECT *
  FROM raw.orders
) TO 'data_lake/orders'
WITH (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE TRUE);

COPY (
  SELECT *
  FROM raw.order_items
) TO 'data_lake/order_items'
WITH (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE TRUE);

COPY (
  SELECT *
  FROM raw.order_item_refunds
) TO 'data_lake/order_item_refunds'
WITH (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE TRUE);

COPY (
  SELECT *
  FROM raw.website_sessions
) TO 'data_lake/website_sessions'
WITH (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE TRUE);

COPY (
  SELECT *
  FROM raw.website_pageviews
) TO 'data_lake/website_pageviews'
WITH (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE TRUE);

COPY (
  SELECT *
  FROM raw.products
) TO 'data_lake/products'
WITH (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE);

COPY (
  SELECT *
  FROM raw.mff_data_dictionary
) TO 'data_lake/mff_data_dictionary'
WITH (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE);

-- 4) Cleanup staging tables
DROP TABLE IF EXISTS raw.stg_orders;
DROP TABLE IF EXISTS raw.stg_order_items;
DROP TABLE IF EXISTS raw.stg_order_item_refunds;
DROP TABLE IF EXISTS raw.stg_website_sessions;
DROP TABLE IF EXISTS raw.stg_website_pageviews;
DROP TABLE IF EXISTS raw.stg_products;
DROP TABLE IF EXISTS raw.stg_mff_data_dictionary;
