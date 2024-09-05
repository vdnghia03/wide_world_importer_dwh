WITH stg_dim_color__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.warehouse__colors`
)

, stg_dim_color__rename_column AS (
  SELECT
    color_id AS color_key
    , color_name AS color_name
  FROM stg_dim_color__source
)

, stg_dim_color__cast_type AS (
  SELECT
    CAST(color_key AS INTEGER) as color_key
    , CAST(color_name AS STRING) as color_name
  FROM stg_dim_color__rename_column
)

, stg_dim_color__undefined_column AS (
  SELECT
    color_key
    , color_name
  FROM stg_dim_color__cast_type
  UNION ALL
  SELECT
    0 as color_key
    , "Undefined" as color_name
  UNION ALL
  SELECT
    -1 as color_key
    , "Invalid" as color_name
)

SELECT
  color_key
  ,color_name
FROM stg_dim_color__undefined_column
