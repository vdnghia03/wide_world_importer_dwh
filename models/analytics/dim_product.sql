WITH dim_product__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

, dim_product__rename_column AS (
  SELECT 
    stock_item_id AS product_key
    , stock_item_name AS product_name
    , brand AS brand_name
    , size AS size_name
    , is_chiller_stock AS is_chiller_stock_boolean
    , unit_price AS unit_price
    , recommended_retail_price AS recommended_retail_price
    , typical_weight_per_unit AS typical_weight_per_unit
    , tax_rate AS tax_rate
    , quantity_per_outer AS quantity_per_outer
    , lead_time_days AS lead_time_days
    , color_id AS color_key
    , unit_package_id AS unit_package_types_key
    , outer_package_id AS outer_package_types_key
    , supplier_id AS supplier_key
  FROM dim_product__source
)

, dim_product__cast_type AS (
  SELECT 
    CAST(product_key AS INTEGER) AS product_key
    , CAST(product_name AS STRING) AS product_name
    , CAST(brand_name AS STRING) AS brand_name
    , CAST(size_name AS STRING) AS size_name
    , CAST(is_chiller_stock_boolean AS BOOLEAN) AS is_chiller_stock_boolean
    , CAST(unit_price AS FLOAT64) AS unit_price
    , CAST(recommended_retail_price AS FLOAT64) AS recommended_retail_price
    , CAST(typical_weight_per_unit AS FLOAT64) AS typical_weight_per_unit
    , CAST(tax_rate AS FLOAT64) AS tax_rate
    , CAST(quantity_per_outer AS INTEGER) AS quantity_per_outer
    , CAST(lead_time_days AS INTEGER) AS lead_time_days
    , CAST(color_key AS INTEGER) AS color_key
    , CAST(unit_package_types_key AS INTEGER) AS unit_package_types_key
    , CAST(outer_package_types_key AS INTEGER) AS outer_package_types_key
    , CAST(supplier_key AS INTEGER) AS supplier_key
  FROM dim_product__rename_column
)

, dim_product__convert_boolean AS (
  SELECT
    *
    , CASE 
        WHEN is_chiller_stock_boolean = TRUE THEN 'Chiller Stock'
        WHEN is_chiller_stock_boolean = FALSE THEN 'Not Chiller Stock'
        WHEN is_chiller_stock_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid'
      END AS is_chiller_stock

  FROM dim_product__cast_type
)

, dim_product__coalesce_record AS (
  SELECT
    product_key
    , COALESCE(product_name, "Undefined") AS product_name
    , COALESCE(brand_name, "Undefined") AS brand_name
    , COALESCE(size_name, "Undefined") AS size_name
    , is_chiller_stock
    , unit_price
    , recommended_retail_price
    , typical_weight_per_unit
    , tax_rate
    , quantity_per_outer
    , lead_time_days
    , COALESCE(color_key, 0) AS color_key
    , COALESCE(unit_package_types_key, 0) AS unit_package_types_key
    , COALESCE(outer_package_types_key, 0) AS outer_package_types_key
    , COALESCE(supplier_key, 0) AS supplier_key
  FROM dim_product__convert_boolean
)

, dim_product__undefine_column AS (
  SELECT
    product_key
    , product_name
    , brand_name
    , size_name
    , is_chiller_stock
    , unit_price
    , recommended_retail_price
    , typical_weight_per_unit
    , tax_rate
    , quantity_per_outer
    , lead_time_days
    , color_key
    , unit_package_types_key
    , outer_package_types_key
    , supplier_key
  FROM dim_product__coalesce_record

  UNION ALL 
  SELECT
    0 as product_key
    , "Undefined" as product_name
    , "Undefined" as brand_name
    , "Undefined" as size_name
    , "Undefined" as is_chiller_stock
    , NULL as unit_price
    , NULL as recommended_retail_price
    , NULL as typical_weight_per_unit
    , NULL as tax_rate
    , NULL as quantity_per_outer
    , NULL as lead_time_days
    , 0 as color_key
    , 0 as unit_package_types_key
    , 0 as outer_package_types_key
    , 0 as supplier_key

  UNION ALL
  SELECT
    -1 as product_key
    , "Invalid" as product_name
    , "Invalid" as brand_name
    , "Invalid" as size_name
    , "Invalid" as is_chiller_stock
    , NULL as unit_price
    , NULL as recommended_retail_price
    , NULL as typical_weight_per_unit
    , NULL as tax_rate
    , NULL as quantity_per_outer
    , NULL as lead_time_days
    , -1 as color_key
    , -1 as unit_package_types_key
    , -1 as outer_package_types_key
    , -1 as supplier_key
)

SELECT 
  dim_product.product_key
  , dim_product.product_name
  , dim_product.brand_name
  , dim_product.size_name
  , dim_product.is_chiller_stock
  , dim_product.unit_price
  , dim_product.recommended_retail_price
  , dim_product.typical_weight_per_unit
  , dim_product.tax_rate
  , dim_product.quantity_per_outer
  , dim_product.lead_time_days
  , dim_product.color_key
  , COALESCE(dim_color.color_name, "Undefined") AS color_name
  , dim_product.unit_package_types_key
  , COALESCE(dim_unit_package_types.package_types_name, "Undefined") AS unit_package_types_name
  , dim_product.outer_package_types_key
  , COALESCE(dim_outer_package_types.package_types_name, "Undefined") AS outer_package_types_name
  , dim_product.supplier_key
  , COALESCE(dim_supplier.supplier_name, "Undefined") AS supplier_name
FROM dim_product__undefine_column AS dim_product
LEFT JOIN {{ ref('stg_dim_color') }} AS dim_color
ON dim_product.color_key = dim_color.color_key
LEFT JOIN {{ ref('dim_package_types') }} AS dim_unit_package_types
ON dim_product.unit_package_types_key = dim_unit_package_types.package_types_key
LEFT JOIN {{ ref('dim_package_types') }} AS dim_outer_package_types
ON dim_product.outer_package_types_key = dim_outer_package_types.package_types_key
LEFT JOIN {{ ref('dim_supplier') }} AS dim_supplier
ON dim_product.supplier_key = dim_supplier.supplier_key