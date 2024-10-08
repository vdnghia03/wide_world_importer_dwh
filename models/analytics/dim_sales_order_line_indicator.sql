
with dim_undersupply_backordered as (
    select
        True as is_undersupply_backordered_boolean
        , 'Under Supply Backorder' as is_undersupply_backordered
        union all
        select
        false as is_undersupply_backordered_boolean
        , 'Not Under Supply Backorder' as is_undersupply_backordered
)

select
    FARM_FINGERPRINT(concat(dim_undersupply_backordered.is_undersupply_backordered
    , ','
    , dim_package_types.package_types_key
    )) as composition_key
    , dim_undersupply_backordered.is_undersupply_backordered_boolean
    , dim_undersupply_backordered.is_undersupply_backordered
    , dim_package_types.package_types_key
    , dim_package_types.package_types_name
from dim_undersupply_backordered
cross join {{ref("dim_package_types")}} as dim_package_types