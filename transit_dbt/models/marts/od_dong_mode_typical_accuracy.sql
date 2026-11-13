{{ config(materialized='table') }}
-- Leave-one-out accuracy of fct_od_dong_mode_typical, per mode, as in
-- od_typical_accuracy: each day predicted by the mean of the other days of its
-- type, at two levels:
--   dong_pair    origin dong to destination dong
--   origin_dong  rides starting in each dong
-- wape_scaled first scales the prediction to the day's actual total.
with x as (
  select service_date, day_type, mode, level, k, sum(rides) as pax
  from (
    select o.service_date, d.day_type, d.mode, level,
      if(d.mode = 'subway', o.subway_passengers, o.bus_passengers) as rides,
      if(level = 'dong_pair',
         concat(o.origin_gu, ' ', o.origin_dong, '|', o.dest_gu, ' ', o.dest_dong),
         concat(o.origin_gu, ' ', o.origin_dong)) as k
    from {{ ref('stg_od_dong_mode') }} o
    join {{ ref('od_dong_mode_typical_days') }} d using (service_date)
    cross join unnest(['dong_pair', 'origin_dong']) as level
  )
  where rides > 0
  group by 1, 2, 3, 4, 5
),
n as (
  select day_type, mode, count(*) as n_days from {{ ref('od_dong_mode_typical_days') }} group by 1, 2
),
tot as (
  select day_type, mode, level, k, sum(pax) as s from x group by 1, 2, 3, 4
),
type_tot as (
  select day_type, mode, level, sum(s) as ts from tot group by 1, 2, 3
),
day_tot as (
  select d.service_date, d.day_type, d.mode, d.level, d.actual, tt.ts, n.n_days,
         d.actual * (n.n_days - 1) / (tt.ts - d.actual) as c
  from (select service_date, day_type, mode, level, sum(pax) as actual from x group by 1, 2, 3, 4) d
  join type_tot tt using (day_type, mode, level)
  join n using (day_type, mode)
  where n.n_days >= 2
),
present as (
  select x.service_date, x.day_type, x.mode, x.level,
         sum(abs((t.s - x.pax) / (d.n_days - 1) - x.pax))       as err,
         sum(abs(d.c * (t.s - x.pax) / (d.n_days - 1) - x.pax)) as err_scaled,
         sum(t.s)                                               as s_present
  from x
  join tot t using (day_type, mode, level, k)
  join day_tot d using (service_date, day_type, mode, level)
  group by 1, 2, 3, 4
)
select
  d.service_date, d.day_type, d.mode, d.level, d.actual,
  round((p.err + (d.ts - p.s_present) / (d.n_days - 1)) / d.actual, 4)              as wape,
  round((p.err_scaled + d.c * (d.ts - p.s_present) / (d.n_days - 1)) / d.actual, 4) as wape_scaled,
  round(d.c - 1, 4)                                                                  as total_diff,
  d.n_days
from day_tot d
join present p using (service_date, day_type, mode, level)
