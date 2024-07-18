WITH stg_dim_customer_category__source AS (
SELECT
  *
FROM `vit-lam-data.wide_world_importers.sales__customers_categories`
)

, stg_dim_customer_category__rename_column AS (
SELECT
  customer_category_id AS customer_category_key
  , customer_category_name AS customer_category_name
FROM stg_dim_customer_category__source
)

, stg_dim_customer_category__cast_type AS (
SELECT
  CAST(customer_category_key AS INTEGER) as customer_category_key
  , CAST(customer_category_name AS STRING) as customer_category_name
FROM stg_dim_customer_category__rename_column
)

SELECT
  customer_category_key
  ,customer_category_name
FROM stg_dim_customer_category__cast_type