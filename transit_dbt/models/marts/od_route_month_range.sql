{{ config(materialized='table') }}
-- First and last month of every route in the route OD, for the API's
-- /api/period/route-range (a withdrawn route moves the page to its months).
-- Built here once a week so the API reads a few KB, not fct_od_stop.
select
  route_name,
  date_trunc(min(service_date), month) as first_month,
  date_trunc(max(service_date), month) as last_month
from {{ ref('fct_od_stop') }}
where route_name is not null
group by route_name
