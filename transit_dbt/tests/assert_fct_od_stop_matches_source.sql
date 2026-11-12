-- Rows and passengers per day in fct_od_stop equal the loaded source.
-- Returns the days that differ (a passing test returns nothing).
with src as (
  select parse_date('%y%m%d', service_date_raw) as service_date,
         count(*) as n, sum(passengers) as p
  from {{ source('staging', 'od_route_stop') }}
  group by 1
),
fct as (
  select service_date, count(*) as n, sum(passengers) as p
  from {{ ref('fct_od_stop') }}
  group by 1
)
select coalesce(src.service_date, fct.service_date) as service_date,
       src.n as src_rows, fct.n as fct_rows, src.p as src_passengers, fct.p as fct_passengers
from src full outer join fct using (service_date)
where src.n is distinct from fct.n or src.p is distinct from fct.p
