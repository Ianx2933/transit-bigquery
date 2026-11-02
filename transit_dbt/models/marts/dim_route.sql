{{ config(materialized='table') }}

-- route_name is not unique per route_no in the source. The name used in the
-- most recent month wins (ties broken alphabetically), and route_name_count
-- records how many names the route has carried, so renames stay visible.
with names as (
  select route_no, route_name, max(service_ym) as last_seen_ym
  from {{ ref('stg_hourly_boarding') }}
  where route_name is not null
  group by 1, 2
),

latest as (
  select route_no, route_name
  from names
  where true
  qualify row_number() over (partition by route_no order by last_seen_ym desc, route_name) = 1
),

counts as (
  select route_no, count(*) as route_name_count
  from names
  group by 1
),

routes as (
  select distinct route_no
  from {{ ref('stg_hourly_boarding') }}
)

select
  r.route_no,
  l.route_name,
  coalesce(c.route_name_count, 0) as route_name_count
from routes r
left join latest l using (route_no)
left join counts c using (route_no)
