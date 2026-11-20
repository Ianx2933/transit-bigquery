-- Same guard for district OD: the period days inside the typical window are
-- the days od_dong_mode_typical_days takes.
with win as (
  select min(service_date) as s, max(service_date) as e from {{ ref('od_dong_mode_typical_days') }}
),
period as (
  select p.service_date, p.day_type, p.mode
  from {{ ref('od_dong_period_days') }} p, win
  where p.service_date between win.s and win.e
),
typical as (
  select service_date, day_type, mode from {{ ref('od_dong_mode_typical_days') }}
)
(select 'period only' as side, * from period except distinct select 'period only', * from typical)
union all
(select 'typical only', * from typical except distinct select 'typical only', * from period)
