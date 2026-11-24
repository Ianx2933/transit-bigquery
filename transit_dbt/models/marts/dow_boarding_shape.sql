{{ config(materialized='table', cluster_by=['route_no', 'stop_key']) }}
-- Mean boardings of each day type per route, stop and hour over every stored
-- month: the least squares solution x_d of SeoulTransitPlatform's
-- solve_dow_hourly_ratio (A x = b, A = days of each type in each month, b =
-- the month's boardings). The API splits one month's total into day types
-- with it (BusMonthDemandRepository): a day of type d carries
-- total × x_d / Σ n_d x_d, n_d from month_day_count. Stops keyed as in
-- stg_dow_hourly_ratio; virtual and blank stops are left out.
select
  trim(route_no)                         as route_no,
  lpad(trim(stop_ars), 5, '0')           as stop_key,
  trim(day_type)                         as day_type,
  hour,
  greatest(coalesce(avg_boarding_passengers, 0), 0) as avg_boardings
from {{ source('staging', 'dow_hourly_ratio') }}
where regexp_contains(trim(stop_ars), r'^[0-9]{1,5}$')
  and trim(stop_ars) != '00000'
