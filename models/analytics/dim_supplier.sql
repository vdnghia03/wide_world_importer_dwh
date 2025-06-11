WITH supplier__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, supplier__rename_column AS (
  SELECT
    supplier_id AS supplier_key
    , supplier_name AS supplier_name
    , bank_account_name AS bank_account_name
    , bank_account_branch AS bank_account_branch
    , bank_account_number AS bank_account_number
    , payment_days AS payment_days
    , supplier_category_id AS supplier_category_key
    , primary_contact_person_id AS primary_contact_person_key
    , alternate_contact_person_id AS alternate_contact_person_key
    , delivery_method_id AS delivery_method_key
    , delivery_city_id AS delivery_city_key
    , postal_city_id AS postal_city_key
  FROM supplier__source
)

, supplier__cast_type AS (
  SELECT
    CAST(supplier_key AS INTEGER) AS supplier_key
    , CAST(supplier_name AS STRING) AS supplier_name
    , CAST(bank_account_name AS STRING) AS bank_account_name
    , CAST(bank_account_branch AS STRING) AS bank_account_branch
    , CAST(bank_account_number AS STRING) AS bank_account_number
    , CAST(payment_days AS INTEGER) AS payment_days
    , CAST(supplier_category_key AS INTEGER) AS supplier_category_key
    , CAST(primary_contact_person_key AS INTEGER) AS primary_contact_person_key
    , CAST(alternate_contact_person_key AS INTEGER) AS alternate_contact_person_key
    , CAST(delivery_method_key AS INTEGER) AS delivery_method_key
    , CAST(delivery_city_key AS INTEGER) AS delivery_city_key
    , CAST(postal_city_key AS INTEGER) AS postal_city_key
  FROM supplier__rename_column
)

, supplier__coalesce_record AS (
  SELECT
    supplier_key
    , COALESCE(supplier_name, "Undefined") AS supplier_name
    , COALESCE(bank_account_name, "Undefined") AS bank_account_name
    , COALESCE(bank_account_branch, "Undefined") AS bank_account_branch
    , bank_account_number
    , payment_days
    , COALESCE(supplier_category_key, 0) AS supplier_category_key
    , COALESCE(primary_contact_person_key, 0) AS primary_contact_person_key
    , COALESCE(alternate_contact_person_key, 0) AS alternate_contact_person_key
    , COALESCE(delivery_method_key, 0) AS delivery_method_key
    , COALESCE(delivery_city_key, 0) AS delivery_city_key
    , COALESCE(postal_city_key, 0) AS postal_city_key
  FROM supplier__cast_type
)

, dim_supplier__undefine_column AS (
  SELECT
    supplier_key
    , supplier_name
    , bank_account_name
    , bank_account_branch
    , bank_account_number
    , payment_days
    , supplier_category_key
    , primary_contact_person_key
    , alternate_contact_person_key
    , delivery_method_key
    , delivery_city_key
    , postal_city_key
  FROM supplier__coalesce_record

  UNION ALL
  SELECT
    0 as supplier_key
    , "Undefined" as supplier_name
    , "Undefined" as bank_account_name
    , "Undefined" as bank_account_branch
    ,  NULL as bank_account_number
    ,  NULL as payment_days
    , 0 as supplier_category_key
    , 0 as primary_contact_person_key
    , 0 as alternate_contact_person_key
    , 0 as delivery_method_key
    , 0 as delivery_city_key
    , 0 as postal_city_key
  UNION ALL
  SELECT
    -1 as supplier_key
    , "Invalid" as supplier_name
    , "Invalid" as bank_account_name
    , "Invalid" as bank_account_branch
    ,  NULL as bank_account_number
    ,  NULL as payment_days
    , -1 as supplier_category_key
    , -1 as primary_contact_person_key
    , -1 as alternate_contact_person_key
    , -1 as delivery_method_key
    , -1 as delivery_city_key
    , -1 as postal_city_key
)

SELECT
  dim_supplier.supplier_key
  , dim_supplier.supplier_name
  , dim_supplier.bank_account_name
  , dim_supplier.bank_account_branch
  , dim_supplier.bank_account_number
  , dim_supplier.payment_days
  , dim_supplier.supplier_category_key
  , COALESCE(dim_supplier_category.supplier_category_name, "Undefined") AS supplier_category_name
  , dim_supplier.primary_contact_person_key
  , COALESCE(dim_primary_contact_person.full_name, "Undefined") AS primary_contact_person_name
  , dim_supplier.alternate_contact_person_key
  , COALESCE(dim_alternate_contact_person.full_name, "Undefined") AS alternate_contact_person_name
  , dim_supplier.delivery_method_key
  , COALESCE(dim_delivery_method.delivery_method_name, "Undefined") AS delivery_method_name
  , dim_supplier.delivery_city_key
  , COALESCE(dim_delivery_city.city_name, "Undefined") AS delivery_city_name
  , dim_delivery_city.state_province_key as delivery_state_province_key
  , dim_delivery_city.state_province_name AS delivery_state_province_name
  , dim_supplier.postal_city_key
  , COALESCE(dim_postal_city.city_name, "Undefined") AS postal_city_name
  , dim_postal_city.state_province_key AS postal_state_province_key
  , dim_postal_city.state_province_name AS postal_state_province_name

FROM dim_supplier__undefine_column AS dim_supplier
LEFT JOIN {{ ref('stg_dim_supplier_category') }} AS dim_supplier_category
  ON dim_supplier.supplier_category_key = dim_supplier_category.supplier_category_key
LEFT JOIN {{ ref( 'dim_person' ) }} AS dim_primary_contact_person
  ON dim_supplier.primary_contact_person_key = dim_primary_contact_person.person_key
LEFT JOIN {{ ref( 'dim_person' ) }} AS dim_alternate_contact_person
  ON dim_supplier.alternate_contact_person_key = dim_alternate_contact_person.person_key
LEFT JOIN {{ ref( 'stg_dim_delivery_method' ) }} AS dim_delivery_method
  ON dim_supplier.delivery_method_key = dim_delivery_method.delivery_method_key
LEFT JOIN {{ ref( 'stg_dim_city' ) }} AS dim_delivery_city
  ON dim_supplier.delivery_city_key = dim_delivery_city.city_key
LEFT JOIN {{ ref( 'stg_dim_city' )}} AS dim_postal_city
  ON dim_supplier.postal_city_key = dim_postal_city.city_key


