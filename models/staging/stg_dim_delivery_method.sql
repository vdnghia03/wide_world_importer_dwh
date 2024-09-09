WITH stg_dim_delivery_method__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.application__delivery_methods`
)

, stg_dim_delivery_method__rename_column AS (
  SELECT
    delivery_method_id AS delivery_method_key
    , delivery_method_name AS delivery_method_name
  FROM stg_dim_delivery_method__source
)

, stg_dim_delivery_method__cast_type AS (
  SELECT
    CAST(delivery_method_key AS INTEGER) as delivery_method_key
    , CAST(delivery_method_name AS STRING) as delivery_method_name
  FROM stg_dim_delivery_method__rename_column
)

, stg_dim_delivery_method__undefined_column AS (
  SELECT
    delivery_method_key
    , delivery_method_name
  FROM stg_dim_delivery_method__cast_type
  UNION ALL
  SELECT
    0 as delivery_method_key
    , "Undefined" as delivery_method_name
  UNION ALL
  SELECT
    -1 as delivery_method_key
    , "Invalid" as delivery_method_name
)

SELECT
  delivery_method_key
  , COALESCE(delivery_method_name, 'Undefined') as delivery_method_name
FROM stg_dim_delivery_method__undefined_column
