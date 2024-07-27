WITH stg_dim_sales_customer_category__source AS (
SELECT
  *
FROM `vit-lam-data.wide_world_importers.sales__customer_categories`
)

, stg_dim_sales_customer_category__rename_column AS (
SELECT
  customer_category_id AS customer_category_key
  , customer_category_name AS customer_category_name
FROM stg_dim_sales_customer_category__source
)

, stg_dim_sales_customer_category__cast_type AS (
SELECT
  CAST(customer_category_key AS INTEGER) as customer_category_key
  , CAST(customer_category_name AS STRING) as customer_category_name
FROM stg_dim_sales_customer_category__rename_column
)

, stg_dim_sales_customer_category__add_undefined_record AS (
SELECT
  customer_category_key
  , customer_category_name
FROM stg_dim_sales_customer_category__cast_type
UNION ALL
SELECT
  0 as customer_category_key
  , "Undefined" as customer_category_name
UNION ALL
SELECT
  -1 as customer_category_key
  , "Invalid" as customer_category_name
)

SELECT
  customer_category_key
  ,customer_category_name
FROM stg_dim_sales_customer_category__add_undefined_record