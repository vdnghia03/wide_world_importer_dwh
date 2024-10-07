
WITH sales_order_line__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, sales_order_line__rename_column AS (
  SELECT
    order_line_id AS sales_order_line_key
    , description AS description_line
    , order_id AS sales_order_key
    , stock_item_id AS product_key
    , package_type_id AS package_type_key
    , picking_completed_when AS line_picking_completed_when
    , quantity  AS quantity
    , unit_price AS unit_price
    , tax_rate AS tax_rate
    , picked_quantity AS picked_quantity
  FROM sales_order_line__source
)

, sales_order_line__cast_type AS (
SELECT
  CAST(sales_order_line_key AS INTEGER) AS sales_order_line_key
  , CAST(description_line AS STRING) AS description_line
  , CAST(sales_order_key AS INTEGER) AS sales_order_key
  , CAST(product_key AS INTEGER) AS product_key
  , CAST(package_type_key AS INTEGER) AS package_type_key
  , CAST(line_picking_completed_when AS TIMESTAMP) AS line_picking_completed_when
  , CAST(quantity AS INTEGER) AS quantity
  , CAST(unit_price AS FLOAT64) AS unit_price
  , CAST(tax_rate AS FLOAT64) AS tax_rate
  , CAST(picked_quantity AS INTEGER) AS picked_quantity
FROM sales_order_line__rename_column
)

, sales_order_line__caculated_measure AS (
  SELECT
    *
    , quantity * unit_price as gross_amount
  FROM sales_order_line__cast_type
)

, sales_order_line__add_tax_amount_measure AS (
  SELECT
    *
    , gross_amount * tax_rate as tax_amount
  FROM sales_order_line__caculated_measure
)

, sales_order_line__add_net_amount_measure AS (
  SELECT
    *
    , gross_amount + tax_amount as net_amount
  FROM sales_order_line__add_tax_amount_measure
)

, sales_order_line__coalesce_column AS (
  SELECT
    sales_order_line_key
    , COALESCE(description_line, 'Undefined') AS description_line
    , COALESCE(sales_order_key, 0) AS sales_order_key
    , COALESCE(product_key, 0) AS product_key
    , COALESCE(package_type_key, 0) AS package_type_key
    , line_picking_completed_when
    , quantity
    , unit_price
    , tax_rate
    , picked_quantity
    , gross_amount
    , tax_amount
    , net_amount
  FROM sales_order_line__add_net_amount_measure
)

, sales_order_line__undefined_record AS (
  SELECT
    sales_order_line_key
    , description_line
    , sales_order_key
    , product_key
    , package_type_key
    , line_picking_completed_when
    , quantity
    , unit_price
    , tax_rate
    , picked_quantity
    , gross_amount
    , tax_amount
    , net_amount
  FROM sales_order_line__coalesce_column
  
  UNION ALL
  SELECT
    0 AS sales_order_line_key
    , 'Undefined' AS description_line
    , 0 AS sales_order_key
    , 0 AS product_key
    , 0 AS package_type_key
    , NULl AS line_picking_completed_when
    , NULL AS quantity
    , NULL AS unit_price
    , NULL AS tax_rate
    , NULL AS picked_quantity
    , NULL AS gross_amount
    , NULL AS tax_amount
    , NULL AS net_amount
  
  UNION ALL
  SELECT
    -1 AS sales_order_line_key
    , 'Unvalid' AS description_line
    , -1 AS sales_order_key
    , -1 AS product_key
    , -1 AS package_type_key
    , NULl AS line_picking_completed_when
    , NULL AS quantity
    , NULL AS unit_price
    , NULL AS tax_rate
    , NULL AS picked_quantity
    , NULL AS gross_amount
    , NULL AS tax_amount
    , NULL AS net_amount
)



SELECT 
  fact_line.sales_order_line_key
  , fact_line.description_line
  , COALESCE(fact_header.is_under_supply_backorder, 'Undefined') as is_under_supply_backorder
  , fact_line.sales_order_key
  , fact_line.product_key
  , fact_line.package_type_key
  , FARM_FINGERPRINT(concat(
      COALESCE(fact_header.is_under_supply_backorder, 'Undefined')
      , ','
      , fact_line.package_type_key
    )) as composition_key
  , COALESCE(fact_header.customer_key, -1) AS customer_key
  , COALESCE(fact_header.sale_person_key,-1) AS sale_person_key
  , COALESCE(fact_header.picked_by_person_key,-1) AS picked_by_person_key
  , COALESCE(fact_header.contact_person_key,-1) AS contact_person_key
  , fact_header.order_date
  , fact_header.expected_delivery_date
  , fact_line.line_picking_completed_when
  , fact_header.order_picking_completed_when
  , fact_line.quantity
  , fact_line.unit_price
  , fact_line.tax_rate
  , fact_line.picked_quantity
  , fact_line.gross_amount
  , fact_line.tax_amount
  , fact_line.net_amount
FROM sales_order_line__undefined_record AS fact_line
LEFT JOIN {{ (ref('stg_fact_sales_order')) }} AS fact_header
ON fact_line.sales_order_key = fact_header.sales_order_key
