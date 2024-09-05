WITH dim_customer__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename_column AS (
  SELECT
    customer_id AS customer_key
    , customer_name AS customer_name
    , is_on_credit_hold AS is_on_credit_hold_boolean
    , is_statement_sent AS is_statement_sent_boolean
    , credit_limit AS credit_limit
    , standard_discount_percentage AS standard_discount_percentage
    , payment_days AS payment_days
    , account_opened_date AS account_opened_date
    , customer_category_id AS customer_category_key
    , buying_group_id AS buying_group_key
    , delivery_method_id AS delivery_method_key
    , delivery_city_id AS delivery_city_key
    , postal_city_id AS postal_city_key
  FROM dim_customer__source
)

, dim_customer__cast_type AS (
  SELECT
    CAST(customer_key as INTEGER) as customer_key
    , CAST(customer_name as STRING) as customer_name
    , CAST(is_on_credit_hold_boolean as BOOLEAN) as is_on_credit_hold_boolean
    , CAST(is_statement_sent_boolean as BOOLEAN) as is_statement_sent_boolean
    , CAST(credit_limit as FLOAT) as credit_limit
    , CAST(standard_discount_percentage as FLOAT) as standard_discount_percentage
    , CAST(payment_days as INTEGER) as payment_days
    , CAST(account_opened_date as DATE) as account_opened_date
    , CAST(customer_category_key as INTEGER) as customer_category_key
    , CAST(buying_group_key as INTEGER) as buying_group_key
    , CAST(delivery_method_key as INTEGER) as delivery_method_key
    , CAST(delivery_city_key as INTEGER) as delivery_city_key
    , CAST(postal_city_key as INTEGER) as postal_city_key
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
  , CASE
      WHEN is_statement_sent_boolean = TRUE THEN 'Statement Sent'
      WHEN is_statement_sent_boolean = FALSE THEN 'Not Statement Sent'
      WHEN is_statement_sent_boolean IS NULL THEN 'Undefined'
      ELSE 'Invalid'
    END AS is_statement_sent
  FROM dim_customer__cast_type
)

, dim_customer__coalesce_column AS (
  SELECT
    customer_key
    , customer_name
    , is_on_credit_hold
    , is_statement_sent
    , credit_limit
    , standard_discount_percentage
    , payment_days
    , account_opened_date
    , COALESCE(customer_category_key, 0) as customer_category_key
    , COALESCE(buying_group_key, 0) as buying_group_key
    , COALESCE(delivery_method_key, 0) as delivery_method_key
    , COALESCE(delivery_city_key, 0) as delivery_city_key
    , COALESCE(postal_city_key, 0) as postal_city_key
  FROM dim_customer__convert_boolean
)

, dim_customer__add_undefined_record AS (
  SELECT
    customer_key
    , customer_name
    , is_on_credit_hold
    , is_statement_sent
    , credit_limit
    , standard_discount_percentage
    , payment_days
    , account_opened_date
    , customer_category_key
    , buying_group_key
    , delivery_method_key
    , delivery_city_key
    , postal_city_key
  FROM dim_customer__coalesce_column

  UNION ALL
  SELECT
    0 as customer_key
    , "Undefined" as customer_name
    , "Undefined" as is_on_credit_hold
    , "Undefined" as is_statement_sent
    , NULL as credit_limit
    , NULL as standard_discount_percentage
    , NULL as payment_days
    , NULL as account_opened_date
    , 0 as customer_category_key
    , 0 as buying_group_key
    , 0 as delivery_method_key
    , 0 as delivery_city_key
    , 0 as postal_city_key

  UNION ALL
  SELECT
    -1 as customer_key
    , "Invalid" as customer_name
    , "Invalid" as is_on_credit_hold
    , "Invalid" as is_statement_sent
    , NULL as credit_limit
    , NULL as standard_discount_percentage
    , NULL as payment_days
    , NULL as account_opened_date
    , -1 as customer_category_key
    , -1 as buying_group_key
    , -1 as delivery_method_key
    , -1 as delivery_city_key
    , -1 as postal_city_key
)



SELECT 
  dim_customer.customer_key
  , dim_customer.customer_name
  , dim_customer.is_on_credit_hold
  , dim_customer.is_statement_sent
  , dim_customer.credit_limit
  , dim_customer.standard_discount_percentage
  , dim_customer.payment_days
  , dim_customer.account_opened_date
  , dim_customer.customer_category_key
  , COALESCE(dim_customer_category.customer_category_name, "Undefined") AS customer_category_name
  , dim_customer.buying_group_key
  , COALESCE(dim_buying_group.buying_group_name, "Undefined") AS buying_group_name
  , dim_customer.delivery_method_key
  , COALESCE(dim_delivery_method.delivery_method_name, "Undefined") AS delivery_method_name
  , dim_customer.delivery_city_key
  , COALESCE(dim_delivery_city.city_name, "Undefined") AS delivery_city_name
  , COALESCE(dim_delivery_city.state_province_key, 0) AS delivery_state_province_key
  , COALESCE(dim_delivery_city.state_province_name, "Undefined") AS delivery_state_province_name
  , dim_customer.postal_city_key
  , COALESCE(dim_postal_city.city_name, "Undefined") AS postal_city_name
  , COALESCE(dim_postal_city.state_province_key, 0) AS postal_state_province_key
  , COALESCE(dim_postal_city.state_province_name, "Undefined") AS postal_state_province_name
FROM dim_customer__add_undefined_record AS dim_customer
LEFT JOIN {{ ref('stg_dim_sales_customer_category') }} AS dim_customer_category
ON dim_customer_category.customer_category_key = dim_customer.customer_category_key
LEFT JOIN {{ ref('stg_dim_sales_buying_group') }} AS dim_buying_group
ON dim_buying_group.buying_group_key = dim_customer.buying_group_key
LEFT JOIN {{ ref('stg_dim_sales_delivery_method') }} AS dim_delivery_method
ON dim_delivery_method.delivery_method_key = dim_customer.delivery_method_key
LEFT JOIN {{ref('stg_dim_city')}} AS dim_delivery_city
ON dim_delivery_city.city_key = dim_customer.delivery_city_key
LEFT JOIN {{ref('stg_dim_city')}} AS dim_postal_city
ON dim_postal_city.city_key = dim_customer.postal_city_key