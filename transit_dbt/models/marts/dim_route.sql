{{ config(materialized='table') }}

-- route_name is not unique per route_no in the source. The name used in the
-- most recent month wins (ties broken alphabetically), and route_name_count
-- records how many names the route has carried, so renames stay visible.
--
-- Source names look like "121번(화계사~서울숲)" or "성동13(옥수역~성수역)":
-- a route number, an optional 번, then origin~terminus in parentheses.
-- route_type is derived from the shape of the route number, following the
-- Seoul numbering scheme. There are no Gyeonggi-registered routes in the source. assert_route_type_classified fails the build if a
-- new shape appears that these rules do not cover.
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
),

parsed as (
  select
    r.route_no,
    l.route_name,
    coalesce(c.route_name_count, 0)                                         as route_name_count,
    regexp_replace(trim(regexp_extract(l.route_name, r'^[^(]*')), r'번$', '') as route_number,
    trim(regexp_extract(l.route_name, r'\(([^~)]*)~'))                       as origin,
    trim(regexp_extract(l.route_name, r'~([^)]*)\)'))                        as terminus
  from routes r
  left join latest l using (route_no)
  left join counts c using (route_no)
)

select
  route_no,
  route_name,
  route_number,
  case
    when route_number is null                          then null
    when starts_with(route_number, '한강버스')          then 'ferry'      -- Han River ferry, paid by transit card
    when regexp_contains(route_number, r'(출근|퇴근)')  then 'commuter'   -- peak-only commuter services
    when regexp_contains(route_number, r'^N\d')        then 'night'      -- 심야
    when regexp_contains(route_number, r'^\p{Hangul}+A\d') then 'autonomous' -- 자율주행: area name + A + number (청와대A01, 상암A21)
    when regexp_contains(route_number, r'^\p{Hangul}') then 'village'    -- 마을버스: area name + number
    when regexp_contains(route_number, r'^9\d{3}')     then 'express'    -- 광역
    when regexp_contains(route_number, r'^8\d{3}')     then 'custom'     -- 맞춤
    when regexp_contains(route_number, r'^\d{4}')      then 'branch'     -- 지선
    when regexp_contains(route_number, r'^\d{3}')      then 'trunk'      -- 간선
    when regexp_contains(route_number, r'^\d{2}')      then 'circular'   -- 순환
    else 'other'
  end                                                  as route_type,
  -- A trailing A/B after the number marks a variant line (계통) of the same
  -- route, e.g. 2115A / 2115B, 702A / 702B. Unrelated to the autonomous A above.
  regexp_extract(route_number, r'\d([AB])$')          as route_variant,
  origin,
  terminus,
  route_name_count
from parsed
