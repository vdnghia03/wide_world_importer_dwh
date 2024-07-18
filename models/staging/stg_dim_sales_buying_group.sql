WITH stg_dim_sales_buying_group__source AS (
SELECT
  *
FROM `vit-lam-data.wide_world_importers.sales__buying_groups`
)

, stg_dim_sales_buying_group__rename_column AS (
SELECT
  buying_group_id AS buying_group_key
  , buying_group_name AS buying_group_name
FROM stg_dim_sales_buying_group__source
)

, stg_dim_sales_buying_group__cast_type AS (
SELECT
  CAST(buying_group_key AS INTEGER) as buying_group_key
  , CAST(buying_group_name AS STRING) as buying_group_name
FROM stg_dim_sales_buying_group__rename_column
)

SELECT
  buying_group_key
  ,buying_group_name
FROM stg_dim_sales_buying_group__cast_type