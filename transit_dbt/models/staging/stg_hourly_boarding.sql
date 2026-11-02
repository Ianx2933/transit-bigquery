-- Source stop names carry the stop's ordinal along the route as a suffix,
-- e.g. 동묘앞(00029). The same stop gets a different suffix on every route
-- that serves it, so the raw name is not a stop attribute. It is split here:
--   stop_base_name  -> the stop's own name (동묘앞)
--   route_stop_seq  -> the ordinal on this route (29), a route-stop attribute
select
  cast(use_ym as int64)                                         as service_ym,
  parse_date('%Y%m%d', concat(use_ym, '01'))                    as service_month,
  hour,
  route_no,
  route_name,
  stop_id,
  stop_ars,
  stop_name,
  trim(regexp_replace(stop_name, r'\(\d{5}\)$', ''))            as stop_base_name,
  safe_cast(regexp_extract(stop_name, r'\((\d{5})\)$') as int64) as route_stop_seq,
  boarding_passengers,
  alighting_passengers
from {{ source('staging', 'hourly_bus_stop_passenger') }}
