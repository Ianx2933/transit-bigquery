select route_no, stop_key, route_stop_seq, count(*) as n
from {{ ref('route_stop_sequence') }}
group by 1, 2, 3
having count(*) > 1
