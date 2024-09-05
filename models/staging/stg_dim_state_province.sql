WITH stg_dim_state_province__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.application__state_provinces`
)

, stg_dim_state_province__rename_column AS (
  SELECT
    state_province_id AS state_province_key
    , state_province_name AS state_province_name
  FROM stg_dim_state_province__source
)

, stg_dim_state_province__cast_type AS (
  SELECT
    CAST(state_province_key AS INTEGER) as state_province_key
    , CAST(state_province_name AS STRING) as state_province_name
  FROM stg_dim_state_province__rename_column
)

, stg_dim_state_province__undefined_column AS (
  SELECT
    state_province_key
    , state_province_name
  FROM stg_dim_state_province__cast_type
  UNION ALL
  SELECT
    0 as state_province_key
    , "Undefined" as state_province_name
  UNION ALL
  SELECT
    -1 as state_province_key
    , "Invalid" as state_province_name
)

SELECT
  state_province_key
  ,state_province_name
FROM stg_dim_state_province__undefined_column