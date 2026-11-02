select
  cast(use_ym as int64)                         as service_ym,
  parse_date('%Y%m%d', concat(use_ym, '01'))    as service_month,
  hour,
  route_no,
  route_name,
  stop_id,
  stop_ars,
  stop_name,
  boarding_passengers,
  alighting_passengers
from {{ source('staging', 'hourly_bus_stop_passenger') }}
