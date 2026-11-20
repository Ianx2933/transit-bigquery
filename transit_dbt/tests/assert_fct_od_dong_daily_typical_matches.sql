-- The typical day averaged from fct_od_dong_daily over od_dong_mode_typical_days
-- equals fct_od_dong_mode_typical summed to codes (what od_dong_export.py sends
-- to Cloud SQL): the period path and the typical path agree. Per day type, total
-- rides by mode; returns day types that differ by more than 1e-6.
with n as (
  select day_type, mode, count(*) as n_days from {{ ref('od_dong_mode_typical_days') }} group by 1, 2
),
period as (
  select f.day_type,
         sum(if(d.mode = 'subway', f.subway_rides, 0) / if(d.mode = 'subway', n.n_days, 1)) as subway,
         sum(if(d.mode = 'bus', f.bus_rides, 0) / if(d.mode = 'bus', n.n_days, 1)) as bus
  from {{ ref('fct_od_dong_daily') }} f
  join {{ ref('od_dong_mode_typical_days') }} d on d.service_date = f.service_date and d.day_type = f.day_type
  join n on n.day_type = d.day_type and n.mode = d.mode
  group by 1
),
typical as (
  select day_type, sum(subway_rides) as subway, sum(bus_rides) as bus
  from {{ ref('fct_od_dong_mode_typical') }} group by 1
)
select t.day_type, t.subway, p.subway as period_subway, t.bus, p.bus as period_bus
from typical t
left join period p using (day_type)
where p.day_type is null
   or abs(t.subway - p.subway) > 1e-6 * greatest(1, t.subway)
   or abs(t.bus - p.bus) > 1e-6 * greatest(1, t.bus)
