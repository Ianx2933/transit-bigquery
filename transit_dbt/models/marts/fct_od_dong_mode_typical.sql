{{ config(materialized='table', cluster_by=['day_type', 'origin_gu', 'origin_dong']) }}
-- Typical-day district OD by mode: mean daily subway and bus rides per dong pair
-- and day type, from TOPIS's district mode OD over od_dong_mode_typical_days
-- (about a year of every day, not only the three route-OD days a month).
-- Both modes come from the same TOPIS file, so both place stops and stations in
-- dongs the same way, which a mode share needs. Bus here agrees with the
-- corrected stop OD to 0.14% in daily totals (od_dong_bus_check_daily).
-- Rides, not trips: a transfer counts once per mode used.
with n as (
  select day_type, mode, count(*) as n_days from {{ ref('od_dong_mode_typical_days') }} group by 1, 2
),
rides as (
  select o.service_date, d.day_type, d.mode, o.origin_gu, o.origin_dong, o.dest_gu, o.dest_dong,
         if(d.mode = 'subway', o.subway_passengers, o.bus_passengers) as rides
  from {{ ref('stg_od_dong_mode') }} o
  join {{ ref('od_dong_mode_typical_days') }} d using (service_date)
),
by_mode as (
  select r.day_type, r.mode, origin_gu, origin_dong, dest_gu, dest_dong,
         sum(r.rides) / any_value(n.n_days) as avg_rides,
         countif(r.rides > 0)               as days_seen
  from rides r join n using (day_type, mode)
  group by 1, 2, 3, 4, 5, 6
)
select
  day_type, origin_gu, origin_dong, dest_gu, dest_dong,
  sum(if(mode = 'subway', avg_rides, 0))                                  as subway_rides,
  sum(if(mode = 'bus', avg_rides, 0))                                     as bus_rides,
  sum(avg_rides)                                                          as total_rides,
  round(safe_divide(sum(if(mode = 'subway', avg_rides, 0)), sum(avg_rides)), 4) as subway_share,
  max(if(mode = 'subway', days_seen, null))                               as subway_days_seen,
  max(if(mode = 'bus', days_seen, null))                                  as bus_days_seen
from by_mode
group by 1, 2, 3, 4, 5
having sum(avg_rides) > 0
