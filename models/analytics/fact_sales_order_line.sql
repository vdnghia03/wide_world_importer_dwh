
WITH sales_order_line__source AS (
SELECT
  *
FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, sales_order_line__rename_column AS (
SELECT
  order_line_id AS sales_order_line_key
  , order_id AS sales_order_key
  , stock_item_id AS product_key
  , quantity  AS quantity
  , unit_price AS unit_price
  , quantity * unit_price as gross_amount
FROM sales_order_line__source
)

, sales_order_line__cast_type AS (
SELECT
  CAST(sales_order_line_key as INTEGER) as sales_order_line_key
  , CAST(sales_order_key AS INTEGER) as sales_order_key
  , CAST(product_key AS INTEGER) as product_key
  , CAST(quantity as INTEGER) AS quantity
  , CAST(unit_price AS NUMERIC) AS unit_price
FROM sales_order_line__rename_column
)

, sales_order_line__caculated_measure AS (
  SELECT
  *
  , quantity * unit_price as gross_amount
  FROM sales_order_line__cast_type
)


SELECT 
  fact_line.sales_order_line_key
  , fact_line.sales_order_key
  , fact_line.product_key
  , fact_header.customer_key
  , fact_line.quantity
  , fact_line.unit_price
  , fact_line.gross_amount
FROM sales_order_line__caculated_measure AS fact_line
JOIN `learn-dbt-425507.learn_dbt_staging.stg_fact_sales_order` AS fact_header
ON fact_line.sales_order_key = fact_header.sales_order_key
