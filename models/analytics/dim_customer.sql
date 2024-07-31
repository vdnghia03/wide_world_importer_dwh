WITH dim_customer__source AS (
SELECT 
  *
FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename_column AS (
SELECT
  customer_id AS customer_key
  , customer_category_id AS customer_category_key
  , buying_group_id AS buying_group_key
  , customer_name 
  , is_on_credit_hold AS is_on_credit_hold_boolean
FROM dim_customer__source
)

, dim_customer__cast_type AS (
SELECT
  CAST(customer_key as INTEGER) as customer_key
  , CAST(customer_category_key as INTEGER) as customer_category_key
  , CAST(buying_group_key as INTEGER) as buying_group_key
  , CAST(customer_name AS STRING) as customer_name
  , CAST(is_on_credit_hold_boolean AS BOOLEAN) as is_on_credit_hold_boolean
FROM dim_customer__rename_column
)

, dim_customer__convert_boolean AS (
  SELECT
  *
  , CASE
      WHEN is_on_credit_hold_boolean = TRUE THEN 'On Credit Hold'
      WHEN is_on_credit_hold_boolean = FALSE THEN 'Not On Credit Hold'
      WHEN is_on_credit_hold_boolean IS NULL THEN 'Undefined'
      ELSE 'Invalid'
    END AS is_on_credit_hold
  FROM dim_customer__cast_type
)

, dim_customer__coalesce_column AS (
SELECT
  customer_key
  , customer_name
  , is_on_credit_hold
  , COALESCE(customer_category_key, 0) as customer_category_key
  , COALESCE(buying_group_key, 0) as buying_group_key
FROM dim_customer__convert_boolean
)

, dim_customer__add_undefined_record AS (
SELECT
  customer_key
  , customer_name
  , is_on_credit_hold
  , customer_category_key
  , buying_group_key
FROM dim_customer__coalesce_column

UNION ALL
SELECT
  0 as customer_key
  , "Undefined" as customer_name
  , "Undefined" as is_on_credit_hold
  , 0 as customer_category_key
  , 0 as buying_group_key

UNION ALL
SELECT
  -1 as customer_key
  , "Invalid" as customer_name
  , "Invalid" as is_on_credit_hold
  , -1 as customer_category_key
  , -1 as buying_group_key
)



SELECT 
  dim_customer.customer_key
  , dim_customer.customer_name
  , dim_customer.is_on_credit_hold
  , dim_customer.customer_category_key
  , COALESCE(dim_customer_category.customer_category_name, "Undefined") AS customer_category_name
  , dim_customer.buying_group_key
  , COALESCE(dim_buying_group.buying_group_name, "Undefined") AS buying_group_name
FROM dim_customer__add_undefined_record AS dim_customer
LEFT JOIN {{ ref('stg_dim_sales_customer_category') }} AS dim_customer_category
ON dim_customer_category.customer_category_key = dim_customer.customer_category_key
LEFT JOIN {{ ref('stg_dim_sales_buying_group') }} AS dim_buying_group
ON dim_buying_group.buying_group_key = dim_customer.buying_group_key