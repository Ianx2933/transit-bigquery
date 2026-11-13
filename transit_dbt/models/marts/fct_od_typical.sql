{{ config(materialized='table', cluster_by=['day_type', 'mode', 'route_name', 'board_key']) }}
-- Typical-day stop-to-stop OD per route and day type: mean daily passengers over
-- the days in od_typical_days. A pair absent on a day counts as 0 that day, so
-- avg_passengers = total / days in the type, and days_seen shows how often the
-- pair occurred. Stops are keyed by ARS, or by name where there is none (ferry
-- piers, some virtual stops). Rows without a route name are left out.
with n as (
  select day_type, mode, count(*) as n_days
  from {{ ref('od_typical_days') }}
  group by 1, 2
),
od_rows as (
  select
    f.day_type, f.mode, f.route_name,
    coalesce(f.board_stop_key, concat('name:', f.board_name))   as board_key,
    coalesce(f.alight_stop_key, concat('name:', f.alight_name)) as alight_key,
    f.board_name, f.alight_name, f.service_date, f.passengers
  from {{ ref('fct_od_stop') }} f
  join {{ ref('od_typical_days') }} d using (service_date, day_type, mode)
  where f.route_name is not null
)
select
  r.day_type, r.mode, r.route_name, r.board_key, r.alight_key,
  any_value(r.board_name)                        as board_name,
  any_value(r.alight_name)                       as alight_name,
  sum(r.passengers) / any_value(n.n_days)        as avg_passengers,
  count(distinct r.service_date)                 as days_seen,
  any_value(n.n_days)                            as n_days
from od_rows r
join n using (day_type, mode)
group by 1, 2, 3, 4, 5
