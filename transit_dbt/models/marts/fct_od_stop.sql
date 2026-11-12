{{ config(
    materialized='table',
    partition_by={'field': 'service_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['mode', 'route_name', 'board_stop_key']
) }}
-- Stop-to-stop OD per route and day. day_type groups days for typical-day OD:
-- weekday / saturday / sunday_holiday. excluded_from_typical marks rows that
-- must not feed a typical day (a bus strike, a ferry suspension), see seed special_days.
with od as (
  select * from {{ ref('stg_od_route_stop') }}
),
special as (
  select
    service_date,
    logical_or(kind = 'public_holiday')                                as is_public_holiday,
    logical_or(kind != 'public_holiday' and scope in ('all', 'bus'))   as excludes_bus,
    logical_or(kind != 'public_holiday' and scope in ('all', 'ferry')) as excludes_ferry
  from {{ ref('special_days') }}
  group by service_date
)
select
  od.service_date,
  case
    when extract(dayofweek from od.service_date) = 1 or coalesce(s.is_public_holiday, false)
      then 'sunday_holiday'
    when extract(dayofweek from od.service_date) = 7 then 'saturday'
    else 'weekday'
  end                                              as day_type,
  coalesce(if(od.mode = 'ferry', s.excludes_ferry, s.excludes_bus), false) as excluded_from_typical,
  od.mode,
  od.route_name,
  od.route_id,
  od.board_seq,
  od.board_ars                                     as board_stop_key,
  od.board_code,
  od.board_name,
  od.alight_seq,
  od.alight_ars                                    as alight_stop_key,
  od.alight_code,
  od.alight_name,
  od.passengers
from od
left join special s using (service_date)
