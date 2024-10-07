
with sales_order_line_indicator as (
    select
        True as is_under_supply_backorder_boolean
        , 'Under Supply Backorder' as is_under_supply_backorder
        union all
        select
        false as is_under_supply_backorder_boolean
        , 'Not Under Supply Backorder' as is_under_supply_backorder
)

select
    concat(dim_indicator.is_under_supply_backorder
    , ','
    , dim_package_types.package_types_key
    ) as composition_key
    , dim_indicator.is_under_supply_backorder_boolean
    , dim_indicator.is_under_supply_backorder
    , dim_package_types.package_types_key
    , dim_package_types.package_types_name
from sales_order_line_indicator as dim_indicator
cross join {{ref("dim_package_types")}} as dim_package_types