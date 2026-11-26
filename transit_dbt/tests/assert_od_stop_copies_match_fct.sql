-- The API's clustered copies of fct_od_stop hold the same rows and passengers.
with f as (select count(*) as n, round(sum(passengers), 1) as p from {{ ref('fct_od_stop') }}),
c as (
  select 'od_stop_by_route' as t, count(*) as n, round(sum(passengers), 1) as p from {{ ref('od_stop_by_route') }}
  union all
  select 'od_stop_by_board', count(*), round(sum(passengers), 1) from {{ ref('od_stop_by_board') }}
  union all
  select 'od_stop_by_alight', count(*), round(sum(passengers), 1) from {{ ref('od_stop_by_alight') }}
)
select c.t, c.n, f.n as fct_n, c.p, f.p as fct_p
from c cross join f
where c.n != f.n or abs(c.p - f.p) > 1
