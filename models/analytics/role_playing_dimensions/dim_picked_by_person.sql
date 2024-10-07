select
    person_key as picked_by_person_key
    , full_name as picked_by_full_name
    , search_name as picked_by_search_name
    , is_employee
from {{ref("dim_person")}}
