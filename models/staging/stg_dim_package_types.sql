WITH stg_dim_package_types__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.warehouse__package_types`
)

, stg_dim_package_types__rename_column AS (
  SELECT
    package_type_id AS package_types_key
    , package_type_name AS package_types_name
  FROM stg_dim_package_types__source
)

, stg_dim_package_types__cast_type AS (
  SELECT
    CAST(package_types_key AS INTEGER) as package_types_key
    , CAST(package_types_name AS STRING) as package_types_name
  FROM stg_dim_package_types__rename_column
)

, stg_dim_package_types__undefined_column AS (
  SELECT
    package_types_key
    , package_types_name
  FROM stg_dim_package_types__cast_type
  UNION ALL
  SELECT
    0 as package_types_key
    , "Undefined" as package_types_name
  UNION ALL
  SELECT
    -1 as package_types_key
    , "Invalid" as package_types_name
)

SELECT
  package_types_key
  , package_types_name
FROM stg_dim_package_types__undefined_column
