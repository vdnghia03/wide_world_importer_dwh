WITH stg_dim_bill_to_customer__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, stg_dim_bill_to_customer__rename_column AS (
  SELECT
    customer_id AS customer_key
    , customer_name AS customer_name
  FROM stg_dim_bill_to_customer__source
)

,stg_dim_bill_to_customer__cast_type AS (
  SELECT
    CAST(customer_key AS INTEGER) as customer_key
    , CAST(customer_name AS STRING) as customer_name
  FROM stg_dim_bill_to_customer__rename_column
)

,stg_dim_bill_to_customer__undefined_column AS (
  SELECT
    customer_key
    , customer_name
  FROM stg_dim_bill_to_customer__cast_type
  UNION ALL
  SELECT
    0 as customer_key
    , "Undefined" as customer_name
  UNION ALL
  SELECT
    -1 as customer_key
    , "Invalid" as customer_name
)

SELECT
  customer_key
  , COALESCE(customer_name, 'Undefined') as customer_name
FROM stg_dim_bill_to_customer__undefined_column
