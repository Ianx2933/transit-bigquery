{{ config(materialized='table') }}
-- A table, not a view: the API reads it on every period request, and a view would
-- scan the whole OD table (hundreds of MB) each time to find a few hundred days.
-- Every route-OD day that may feed a period average, per mode, with its month:
-- od_typical_days without its twelve-month window, same rules otherwise. A day
-- in special_days for a mode (strike, ferry suspension, Seollal and Chuseok) is
-- left out, a public holiday counts as sunday_holiday, the ferry counts from its
-- first day in the data. The API averages a pair over the n days of the chosen
-- months and day type (a pair absent on a day counts as 0), the rule
-- fct_od_typical uses for the typical day; assert_od_period_days_cover_typical
-- checks the two agree on the typical window.
with days as (
  select distinct service_date, day_type from {{ ref('fct_od_stop') }}
),
modes as (
  select 'bus' as mode, date '1900-01-01' as first_day
  union all
  select 'ferry', min(service_date) from {{ ref('fct_od_stop') }} where mode = 'ferry'
),
excluded as (
  select service_date, scope from {{ ref('special_days') }} where kind != 'public_holiday'
)
select d.service_date, date_trunc(d.service_date, month) as month, d.day_type, m.mode
from days d
cross join modes m
left join excluded e
  on e.service_date = d.service_date and e.scope in ('all', m.mode)
where e.service_date is null
  and d.service_date >= m.first_day
