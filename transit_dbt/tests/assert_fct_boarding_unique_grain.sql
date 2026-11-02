-- Same grain as the PostgreSQL primary key (use_ym, route_no, stop_ars, hour).
select service_ym, route_no, stop_key, hour, count(*) as n
from {{ ref('fct_boarding') }}
group by 1, 2, 3, 4
having count(*) > 1
