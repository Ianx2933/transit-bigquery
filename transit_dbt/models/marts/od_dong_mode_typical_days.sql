{{ config(materialized='view') }}
-- Days that feed the typical-day district mode OD: every day TOPIS published in
-- the last var('od_typical_months', 12) calendar months up to its latest day,
-- minus the Seollal and Chuseok breaks. The bus column also drops days in
-- special_days for buses (the 2026-01-13 strike); subway keeps them.
with loaded as (
  select distinct service_date from {{ ref('stg_od_dong_mode') }}
),
bounds as (
  select
    max(service_date) as end_date,
    date_sub(date_trunc(max(service_date), month),
             interval {{ var('od_typical_months', 12) - 1 }} month) as start_date
  from loaded
)
select l.service_date, c.day_type, mode
from loaded l
cross join bounds b
join {{ ref('dim_calendar') }} c on c.calendar_date = l.service_date
cross join unnest(['subway', 'bus']) as mode
where l.service_date between b.start_date and b.end_date
  and not c.is_major_holiday
  and not (mode = 'bus' and c.excludes_bus)
