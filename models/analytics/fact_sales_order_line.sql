SELECT 
  cast(order_line_id as INT) as sales_order_line_key
  , CAST(stock_item_id AS INT) as product_key
  , CAST(quantity as INT) AS quantity
  , CAST(unit_price AS NUMERIC) AS unit_price
  , CAST(quantity AS INT) * CAST(unit_price AS NUMERIC) as gross_amount
FROM `vit-lam-data.wide_world_importers.sales__order_lines`
