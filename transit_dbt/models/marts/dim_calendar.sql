{{ config(materialized='table') }}
-- One row per day, 2023-01-01 to 2027-12-31, with the day type every OD model uses.
--   day_type          weekday / saturday / sunday_holiday (a public holiday is sunday_holiday)
--   is_major_holiday  a public holiday within 3 days of one named 설날 or 추석: the
--                     whole Seollal or Chuseok break, bridge and substitute days
--                     included (2025-01-27, 2023-10-02). Its travel is unlike any
--                     Sunday's, so typical-day models leave it out.
--   excludes_bus / excludes_ferry  seed special_days (strikes, ferry suspensions)
with days as (
  select d as calendar_date
  from unnest(generate_date_array('2023-01-01', '2027-12-31')) as d
),
holidays as (
  select holiday_date, holiday_name from {{ ref('korea_public_holidays') }}
),
major as (
  select distinct h.holiday_date
  from holidays h
  join holidays m
    on (m.holiday_name like '%설날%' or m.holiday_name like '%추석%')
   and abs(date_diff(h.holiday_date, m.holiday_date, day)) <= 3
),
special as (
  select
    service_date,
    logical_or(kind not in ('public_holiday', 'major_holiday') and scope in ('all', 'bus'))   as excludes_bus,
    logical_or(kind not in ('public_holiday', 'major_holiday') and scope in ('all', 'ferry')) as excludes_ferry
  from {{ ref('special_days') }}
  group by 1
)
select
  d.calendar_date,
  extract(dayofweek from d.calendar_date)                       as day_of_week,
  case
    when extract(dayofweek from d.calendar_date) = 1 or h.holiday_date is not null then 'sunday_holiday'
    when extract(dayofweek from d.calendar_date) = 7 then 'saturday'
    else 'weekday'
  end                                                           as day_type,
  h.holiday_name,
  h.holiday_date is not null                                    as is_public_holiday,
  m.holiday_date is not null                                    as is_major_holiday,
  coalesce(s.excludes_bus, false)                               as excludes_bus,
  coalesce(s.excludes_ferry, false)                             as excludes_ferry
from days d
left join holidays h on h.holiday_date = d.calendar_date
left join major m on m.holiday_date = d.calendar_date
left join special s on s.service_date = d.calendar_date
