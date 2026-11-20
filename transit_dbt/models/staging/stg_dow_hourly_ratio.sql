-- Hourly boarding profile per route, stop and ratio day type, as Cloud SQL's
-- dow_hourly_ratio holds it. The stop is keyed the way estimate_hourly_od.py
-- normalizes ARS: digits padded to five, blank as '00000', anything else as is.
select
  trim(route_no)                                                  as route_no,
  case
    when trim(stop_ars) = '' then '00000'
    when regexp_contains(trim(stop_ars), r'^\d+$') then lpad(trim(stop_ars), 5, '0')
    else trim(stop_ars)
  end                                                             as stop_key,
  trim(day_type)                                                  as ratio_day_type,
  hour,
  boarding_ratio,
  method
from {{ source('staging', 'dow_hourly_ratio') }}
