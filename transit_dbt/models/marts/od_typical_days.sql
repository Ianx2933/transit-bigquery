{{ config(materialized='view') }}
-- The OD days that feed the typical day, per mode: the last
-- var('od_typical_months', 12) calendar months up to the latest OD day, minus
-- special days for the modes they affect (seed special_days; a public holiday
-- is not excluded, it is a sunday_holiday day). The ferry only counts from its
-- first day in the data.
with bounds as (
  select
    max(service_date) as end_date,
    date_sub(date_trunc(max(service_date), month),
             interval {{ var('od_typical_months', 12) - 1 }} month) as start_date
  from {{ ref('fct_od_stop') }}
),
days as (
  select distinct f.service_date, f.day_type
  from {{ ref('fct_od_stop') }} f, bounds b
  where f.service_date between b.start_date and b.end_date
),
modes as (
  select 'bus' as mode, date '1900-01-01' as first_day
  union all
  select 'ferry', min(service_date) from {{ ref('fct_od_stop') }} where mode = 'ferry'
),
excluded as (
  select service_date, scope from {{ ref('special_days') }} where kind != 'public_holiday'
)
select d.service_date, d.day_type, m.mode
from days d
cross join modes m
left join excluded e
  on e.service_date = d.service_date and e.scope in ('all', m.mode)
where e.service_date is null
  and d.service_date >= m.first_day
