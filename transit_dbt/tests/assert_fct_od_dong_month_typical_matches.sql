-- The month table over the typical window's months, divided by the typical
-- days per mode, equals fct_od_dong_mode_typical: the API's period path and
-- the typical day agree when the period is the typical window. Per day type,
-- total rides by mode; returns day types that differ by more than 1e-6.
with win as (
  select date_trunc(min(service_date), month) as s, date_trunc(max(service_date), month) as e
  from {{ ref('od_dong_mode_typical_days') }}
),
n as (
  select p.day_type, countif(p.mode = 'subway') as ns, countif(p.mode = 'bus') as nb
  from {{ ref('od_dong_period_days') }} p, win
  where p.month between win.s and win.e
  group by 1
),
period as (
  select m.day_type, sum(m.subway_rides) / any_value(n.ns) as subway, sum(m.bus_rides) / any_value(n.nb) as bus
  from {{ ref('fct_od_dong_month') }} m
  join n using (day_type)
  cross join win
  where m.month between win.s and win.e
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
