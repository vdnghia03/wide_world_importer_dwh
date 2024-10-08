WITH fact_sales_order__source AS (
SELECT
  *
FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, fact_sales_order__rename_column AS (
SELECT
  order_id AS sales_order_key 
  , is_undersupply_backordered AS is_undersupply_backordered_boolean
  , customer_id AS customer_key
  , salesperson_person_id AS sale_person_key
  , picked_by_person_id AS picked_by_person_key
  , contact_person_id AS contact_person_key
  , order_date
  , expected_delivery_date
  , picking_completed_when AS order_picking_completed_when
FROM fact_sales_order__source
)

, fact_sales_order__cast_type AS (
SELECT
  CAST(sales_order_key AS INTEGER) AS sales_order_key
  , CAST(is_undersupply_backordered_boolean AS BOOLEAN) AS is_undersupply_backordered_boolean
  , CAST(customer_key AS INTEGER) AS customer_key
  , CAST(sale_person_key AS INTEGER) AS sale_person_key
  , CAST(picked_by_person_key AS INTEGER) AS picked_by_person_key
  , CAST(contact_person_key AS INTEGER) AS contact_person_key
  , CAST(order_date AS DATE) AS order_date
  , CAST(expected_delivery_date AS DATE) AS expected_delivery_date
  , CAST(order_picking_completed_when AS TIMESTAMP) AS order_picking_completed_when
FROM fact_sales_order__rename_column
)

, fact_sales_order__convert_boolean AS (
  SELECT
    *
    , CASE 
        WHEN is_undersupply_backordered_boolean = TRUE THEN "Under Supply Backorder"
        WHEN is_undersupply_backordered_boolean = FALSE THEN "Not Under Supply Backorder"
        WHEN is_undersupply_backordered_boolean IS NULL THEN "Undefined"
        ELSE "Invalid"
      END AS is_undersupply_backordered
  FROM fact_sales_order__cast_type
)

, fact_sales_order__coalesce_record AS (
  SELECT
    sales_order_key
    , COALESCE(is_undersupply_backordered, "Undefined") AS is_undersupply_backordered
    , COALESCE(customer_key, 0) AS customer_key
    , COALESCE(sale_person_key, 0) AS sale_person_key
    , COALESCE(picked_by_person_key, 0) AS picked_by_person_key
    , COALESCE(contact_person_key, 0) AS contact_person_key
    , order_date
    , expected_delivery_date
    , order_picking_completed_when
  FROM fact_sales_order__convert_boolean
)

, fact_sales_order__undefined_record AS (
  SELECT
    sales_order_key
    , is_undersupply_backordered
    , customer_key
    , sale_person_key
    , picked_by_person_key
    , contact_person_key
    , order_date
    , expected_delivery_date
    , order_picking_completed_when
  FROM fact_sales_order__coalesce_record
  
  UNION ALL
  SELECT
    0 AS sales_order_key
    , "Undefined" AS is_undersupply_backordered
    , 0 AS customer_key
    , 0 AS sale_person_key
    , 0 AS picked_by_person_key
    , 0 AS contact_person_key
    , NULL AS order_date
    , NULL AS expected_delivery_date
    , NULL AS order_picking_completed_when

  UNION ALL
  SELECT
    -1 AS sales_order_key
    , "Invalid" AS is_undersupply_backordered
    , -1 AS customer_key
    , -1 AS sale_person_key
    , -1 AS picked_by_person_key
    , -1 AS contact_person_key
    , NULL AS order_date
    , NULL AS expected_delivery_date
    , NULL AS order_picking_completed_when
)


SELECT 
  sales_order_key
  , is_undersupply_backordered
  , customer_key
  , sale_person_key
  , picked_by_person_key
  , contact_person_key
  , order_date
  , expected_delivery_date
  , order_picking_completed_when
FROM fact_sales_order__undefined_record