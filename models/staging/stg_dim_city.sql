WITH stg_dim_city__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.application__cities`
)

, stg_dim_city__rename_column AS (
  SELECT
    city_id AS city_key
    , city_name AS city_name
    , state_province_id AS state_province_key
  FROM stg_dim_city__source
)

, stg_dim_city__cast_type AS (
  SELECT
    CAST(city_key AS INTEGER) as city_key
    , CAST(city_name AS STRING) as city_name
    , CAST(state_province_key AS INTEGER) as state_province_key
  FROM stg_dim_city__rename_column
)

, stg_dim_city__coalesce_column AS (
  SELECT
    city_key
    , city_name
    , COALESCE(state_province_key, 0) as state_province_key
  FROM stg_dim_city__cast_type
)

, stg_dim_city__undefined_column AS (
  SELECT
    city_key
    , city_name
    , state_province_key
  FROM stg_dim_city__coalesce_column
  UNION ALL
  SELECT
    0 as city_key
    , "Undefined" as city_name
  UNION ALL
  SELECT
    -1 as city_key
    , "Invalid" as city_name
)

SELECT
  stg_dim_city.city_key
  , stg_dim_city.city_name
  , stg_dim_city.state_province_key
  , COALESCE(stg_dim_state_province.state_province_name, 'Undefined') as state_province_name
FROM stg_dim_city__undefined_column AS stg_dim_city
LEFT JOIN {{ ref('stg_dim_state_province') }} AS stg_dim_state_province
ON stg_dim_city.state_province_key = stg_dim_state_province.state_province_key
