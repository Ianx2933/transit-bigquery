{{ config(materialized='table') }}
-- Leave-one-out accuracy of the typical day. Each day in od_typical_days is
-- predicted by the mean of the other days of its type, and compared with what
-- it actually had, at four levels:
--   od_pair     route, boarding stop, alighting stop
--   route_stop  boardings per route and stop
--   stop        boardings per stop, all routes
--   route       passengers per route
-- Reported per mode: a day can be excluded for one mode only (the 2026-01-13 bus
-- strike left the ferry), and mixing modes would score such a day on a few riders.
--
-- wape        = sum |predicted - actual| / sum actual
-- wape_scaled   the same after scaling the day's prediction to its actual total
--               (c = actual total / predicted total, per mode). It separates "how
--               many rode that day" from "where they went". A day's total is known
--               for every day of the year, not only OD days: TOPIS's district bus
--               OD matches the stop OD total within 0.14% (od_dong_bus_check_daily).
--
-- Keys absent on the day count as actual 0; their error is summed from totals
-- instead of a full key x day grid: for key k with total S_k over n days, the
-- prediction for day d is (S_k - x_kd) / (n - 1).
with x as (
  select service_date, day_type, mode, level, k, sum(passengers) as pax
  from (
    select f.service_date, f.day_type, f.mode, f.passengers, level,
      case level
        when 'od_pair'    then concat(f.route_name, '|', bk, '|', ak)
        when 'route_stop' then concat(f.route_name, '|', bk)
        when 'stop'       then bk
        else f.route_name
      end as k
    from (
      select f.*,
        coalesce(f.board_stop_key, concat('name:', f.board_name))   as bk,
        coalesce(f.alight_stop_key, concat('name:', f.alight_name)) as ak
      from {{ ref('fct_od_stop') }} f
      join {{ ref('od_typical_days') }} using (service_date, day_type, mode)
      where f.route_name is not null
    ) f
    cross join unnest(['od_pair', 'route_stop', 'stop', 'route']) as level
  )
  group by 1, 2, 3, 4, 5
),
n as (
  select day_type, mode, count(*) as n_days from {{ ref('od_typical_days') }} group by 1, 2
),
tot as (
  select day_type, mode, level, k, sum(pax) as s from x group by 1, 2, 3, 4
),
type_tot as (
  select day_type, mode, level, sum(s) as ts from tot group by 1, 2, 3
),
day_tot as (
  select
    d.service_date, d.day_type, d.mode, d.level, d.actual, tt.ts, n.n_days,
    d.actual * (n.n_days - 1) / (tt.ts - d.actual) as c
  from (select service_date, day_type, mode, level, sum(pax) as actual from x group by 1, 2, 3, 4) d
  join type_tot tt using (day_type, mode, level)
  join n using (day_type, mode)
  where n.n_days >= 2
),
present as (
  select
    x.service_date, x.day_type, x.mode, x.level,
    sum(abs((t.s - x.pax) / (d.n_days - 1) - x.pax))         as err,
    sum(abs(d.c * (t.s - x.pax) / (d.n_days - 1) - x.pax))   as err_scaled,
    sum(t.s)                                                 as s_present
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
