
WITH dim_person__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename_column AS (
  SELECT
    person_id AS person_key
    , full_name
    , preferred_name
    , search_name
    , is_permitted_to_logon
    , logon_name
    , is_system_user
    , is_employee
    , is_salesperson
  FROM dim_person__source
)

, dim_person__cast_type AS (
  SELECT
    CAST(person_key AS INT) AS person_key
    , CAST(full_name AS STRING) AS full_name
    , CAST(preferred_name AS STRING) AS preferred_name
    , CAST(search_name AS STRING) AS search_name
    , CAST(is_permitted_to_logon AS BOOLEAN) AS is_permitted_to_logon_boolean
    , CAST(logon_name AS STRING) AS logon_name
    , CAST(is_system_user AS BOOLEAN) AS is_system_user_boolean
    , CAST(is_employee AS BOOLEAN) AS is_employee_boolean
    , CAST(is_salesperson AS BOOLEAN) AS is_salesperson_boolean
  FROM dim_person__rename_column
)

, dim_person_convert_boolean AS (
  SELECT
    *
    , CASE
        WHEN is_permitted_to_logon_boolean = TRUE THEN 'Permitted to Logon'
        WHEN is_permitted_to_logon_boolean = FALSE THEN 'Not Permitted to Logon'
        WHEN is_permitted_to_logon_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid'
      END AS is_permitted_to_logon
    , CASE
        WHEN is_system_user_boolean = TRUE THEN 'System User'
        WHEN is_system_user_boolean = FALSE THEN 'Not System User'
        WHEN is_system_user_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid'
      END AS is_system_user
    , CASE
        WHEN is_employee_boolean = TRUE THEN 'Employee'
        WHEN is_employee_boolean = FALSE THEN 'Not Employee'
        WHEN is_employee_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid'
      END AS is_employee
    , CASE
        WHEN is_salesperson_boolean = TRUE THEN 'Salesperson'
        WHEN is_salesperson_boolean = FALSE THEN 'Not Salesperson'
        WHEN is_salesperson_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid'
      END AS is_salesperson
  FROM dim_person__cast_type
)

, dim_person__add_undefined_record AS (
  SELECT
    person_key
    , full_name
    , preferred_name
    , search_name
    , is_permitted_to_logon
    , logon_name
    , is_system_user
    , is_employee
    , is_salesperson
  FROM dim_person__cast_type

  UNION ALL 
  SELECT
    0 as person_key
    , "Undefined" as full_name
    , "Undefined" as preferred_name
    , "Undefined" as search_name
    , "Undefined" as is_permitted_to_logon
    , "Undefined" as logon_name
    , "Undefined" as is_system_user
    , "Undefined" as is_employee
    , "Undefined" as is_salesperson

  UNION ALL
  SELECT
    -1 as person_key
    , "Invalid" as full_name
    , "Invalid" as preferred_name
    , "Invalid" as search_name
    , "Invalid" as is_permitted_to_logon
    , "Invalid" as logon_name
    , "Invalid" as is_system_user
    , "Invalid" as is_employee
    , "Invalid" as is_salesperson
)


SELECT
  person_key
  , full_name
  , preferred_name
  , search_name
  , is_permitted_to_logon
  , logon_name
  , is_system_user
  , is_employee
  , is_salesperson
FROM dim_person__add_undefined_record
