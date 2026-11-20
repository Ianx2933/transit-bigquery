{{ config(
    materialized='table',
    partition_by={'field': 'month', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['day_type', 'origin_cd', 'dest_cd']
) }}
-- District OD summed per month and day type over the days a period may use
-- (od_dong_period_days: subway every loaded day but Seollal and Chuseok, bus
-- also without bus special days). The API's period query reads the chosen
-- months of this table (a few MB a month, not the whole day table) and divides
-- by the number of those days per mode from od_dong_period_days, which gives
-- the same mean as fct_od_dong_mode_typical over the same days
-- (assert_fct_od_dong_month_typical_matches).
with days as (
  select service_date, mode from {{ ref('od_dong_period_days') }}
)
select
  date_trunc(f.service_date, month)                              as month,
  f.day_type,
  f.origin_cd,
  any_value(f.origin_name)                                       as origin_name,
  f.dest_cd,
  any_value(f.dest_name)                                         as dest_name,
  sum(if(s.service_date is not null, f.subway_rides, 0))         as subway_rides,
  sum(if(b.service_date is not null, f.bus_rides, 0))            as bus_rides
from {{ ref('fct_od_dong_daily') }} f
left join (select service_date from days where mode = 'subway') s using (service_date)
left join (select service_date from days where mode = 'bus') b using (service_date)
where s.service_date is not null or b.service_date is not null
group by 1, 2, 3, 5
