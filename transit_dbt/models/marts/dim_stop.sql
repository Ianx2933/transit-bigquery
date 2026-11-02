{{ config(materialized='table') }}

-- One row per stop_ars, keyed to match fct_boarding.stop_key.
--
-- Three populations, recorded in match_method rather than filtered away:
--   ars_exact          -> matched to the Seoul stop master
--   gyeonggi_unmatched -> Gyeonggi-do stops on cross-boundary routes,
--                         outside the Seoul master's scope
--   virtual_terminal   -> route origin/terminus markers, ARS is literally '~'
-- Unmatched stops are kept so totals reconcile against the source.
--
-- Names are compared without the route ordinal suffix (see stg_hourly_boarding);
-- otherwise every stop served by several routes looks renamed. The ordinal
-- itself lives in route_stop_sequence, not here.
with stop_names as (
  select stop_ars, stop_id, stop_base_name, max(service_ym) as last_seen_ym
  from {{ ref('stg_hourly_boarding') }}
  group by 1, 2, 3
),

latest as (
  select stop_ars, stop_id, stop_base_name
  from stop_names
  where true
  qualify row_number() over (
    partition by stop_ars order by last_seen_ym desc, stop_base_name, stop_id
  ) = 1
),

name_counts as (
  select stop_ars, count(distinct stop_base_name) as stop_name_count
  from stop_names
  group by 1
),

-- The master can list more than one node per ARS; pick one deterministically.
location as (
  select *
  from {{ ref('stg_bus_stop_location') }}
  where stop_ars is not null
  qualify row_number() over (partition by stop_ars order by node_id) = 1
)

select
  s.stop_ars                                   as stop_key,
  s.stop_ars,
  s.stop_id,
  l.node_id,
  s.stop_base_name                             as stop_name,
  n.stop_name_count,
  case
    when s.stop_ars = '~'       then 'virtual_terminal'
    when l.stop_ars is not null then 'ars_exact'
    else 'gyeonggi_unmatched'
  end                                          as match_method,
  l.lon,
  l.lat,
  st_geogpoint(l.lon, l.lat)                   as geog,
  t.elev_m,
  t.slope_pct,
  t.dataset_id
from latest s
join name_counts n using (stop_ars)
left join location l on l.stop_ars = s.stop_ars
left join {{ ref('stg_bus_stop_terrain') }} t on t.node_id = l.node_id
