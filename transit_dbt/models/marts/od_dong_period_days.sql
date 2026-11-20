{{ config(materialized='table') }}
-- A table, not a view: the API reads it on every period request, and a view would
-- scan the whole OD table (hundreds of MB) each time to find a few hundred days.
-- Every district-OD day that may feed a period average, per mode: the days
-- od_dong_mode_typical_days would take if its window were the chosen months
-- (Seollal and Chuseok breaks out, bus also without special_days bus days).
with loaded as (
  select distinct service_date from {{ ref('stg_od_dong_mode') }}
)
select l.service_date, date_trunc(l.service_date, month) as month, c.day_type, mode
from loaded l
join {{ ref('dim_calendar') }} c on c.calendar_date = l.service_date
cross join unnest(['subway', 'bus']) as mode
where not c.is_major_holiday
  and not (mode = 'bus' and c.excludes_bus)
