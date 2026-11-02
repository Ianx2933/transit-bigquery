-- Every named route must fall into a known type. A new numbering shape in a
-- future month lands in 'other' and fails here, so the rules get updated
-- instead of the route being silently miscounted.
select route_no, route_name, route_number, route_type
from {{ ref('dim_route') }}
where route_name is not null
  and (route_type is null or route_type = 'other')
