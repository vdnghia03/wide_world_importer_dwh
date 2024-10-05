
select
    person_key as salesperson_person_key
    , full_name as salesperson_full_name
    , search_name as salesperson_search_name
    , is_employee
    , is_salesperson
from {{ref("dim_person")}}
where
    is_salesperson = 'Salesperson'
    or person_key in (0,-1)