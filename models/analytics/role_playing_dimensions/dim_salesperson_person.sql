
select
    person_key as salesperson_person_key
    , full_name as salesperson_full_name
    , preferred_name
    , search_name
    , is_permitted_to_logon
    , logon_name
    , is_system_user
    , is_employee
    , is_salesperson
from {{ref("dim_person")}} 