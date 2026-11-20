{{ config(materialized='table', cluster_by=['day_type', 'route_no', 'stop_key']) }}
-- The boarding-hour profile OD is split by, per route, boarding stop and OD day
-- type (weekday / saturday / sunday_holiday), 24 rows that sum to one. Same
-- rules as SeoulTransitPlatform's estimate_hourly_od.py, so BigQuery splits a
-- period's OD the way estimated_hourly_od splits the typical weekday:
--   * a fitted profile is scaled to sum to one (build_ratio_map)
--   * weekday = mean of the fitted Monday..Friday profiles (add_weekday_mean)
--   * a key whose fit collapsed to all zeros takes the default profile,
--     method default_static_ratio_zero_fit
-- A route stop with no row here takes the default profile with method
-- default_static_ratio; the query that splits OD adds those (no key to list).
with ratio as (
  select route_no, stop_key, ratio_day_type, hour, boarding_ratio,
         sum(boarding_ratio) over (partition by route_no, stop_key, ratio_day_type) as key_sum
  from {{ ref('stg_dow_hourly_ratio') }}
),
fitted as (
  select route_no, stop_key, ratio_day_type, hour, boarding_ratio / key_sum as ratio
  from ratio
  where key_sum > 0
),
zero_keys as (
  select distinct route_no, stop_key, ratio_day_type from ratio where key_sum <= 0
),
weekday as (
  select route_no, stop_key, hour, avg(ratio) as ratio
  from fitted
  where ratio_day_type in ('mon', 'tue', 'wed', 'thu', 'fri')
  group by 1, 2, 3
),
default_ratio as (
  select hour, raw_ratio / sum(raw_ratio) over () as ratio from {{ ref('default_hourly_ratio') }}
),
weekday_zero as (
  select distinct z.route_no, z.stop_key
  from zero_keys z
  where z.ratio_day_type in ('mon', 'tue', 'wed', 'thu', 'fri')
    and not exists (select 1 from weekday w where w.route_no = z.route_no and w.stop_key = z.stop_key)
),
weekend as (
  select route_no, stop_key,
         case ratio_day_type when 'sat' then 'saturday' else 'sunday_holiday' end as day_type,
         hour, ratio
  from fitted
  where ratio_day_type in ('sat', 'sun_holiday')
),
weekend_zero as (
  select route_no, stop_key, case ratio_day_type when 'sat' then 'saturday' else 'sunday_holiday' end as day_type
  from zero_keys
  where ratio_day_type in ('sat', 'sun_holiday')
)
select route_no, stop_key, 'weekday' as day_type, hour, ratio, 'dow_least_squares_sun_holiday' as method from weekday
union all
select route_no, stop_key, day_type, hour, ratio, 'dow_least_squares_sun_holiday' from weekend
union all
select z.route_no, z.stop_key, 'weekday', d.hour, d.ratio, 'default_static_ratio_zero_fit'
from weekday_zero z cross join default_ratio d
union all
select z.route_no, z.stop_key, z.day_type, d.hour, d.ratio, 'default_static_ratio_zero_fit'
from weekend_zero z cross join default_ratio d
