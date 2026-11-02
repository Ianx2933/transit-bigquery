select stop_key, stop_name, count(*) as n
from {{ ref('dim_stop_name_history') }}
group by 1, 2
having count(*) > 1
