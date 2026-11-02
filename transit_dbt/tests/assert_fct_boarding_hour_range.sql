select service_ym, route_no, stop_key, hour
from {{ ref('fct_boarding') }}
where hour not between 0 and 23
