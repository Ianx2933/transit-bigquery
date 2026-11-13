{{ config(materialized='table') }}
-- fct_od_typical in the shape Cloud SQL serves (od_typical_route_stop): one row
-- per day type, route, boarding stop and alighting stop, with the route ID, stop
-- sequences and standard codes the API and the hourly allocation use. Those
-- attributes are taken from the latest day in the window on which the pair
-- occurred, so they describe the route as it runs now.
-- label is what Cloud SQL stores in 기준일자 (typical_weekday, ...), so code that
-- selects by 기준일자 can ask for a typical day the way it asked for a date.
-- Exported by od/od_typical_export.py, loaded by SeoulTransitPlatform's
-- pipelines/od_typical/load_typical_od.py.
with latest as (
  select
    f.route_name,
    coalesce(f.board_stop_key, concat('name:', f.board_name))   as board_key,
    coalesce(f.alight_stop_key, concat('name:', f.alight_name)) as alight_key,
    array_agg(struct(f.route_id, f.board_seq, f.board_code, f.alight_seq, f.alight_code)
              order by f.service_date desc limit 1)[offset(0)]   as l
  from {{ ref('fct_od_stop') }} f
  join {{ ref('od_typical_days') }} using (service_date, day_type, mode)
  where f.route_name is not null
  group by 1, 2, 3
),
win as (
  select min(service_date) as window_start, max(service_date) as window_end
  from {{ ref('od_typical_days') }}
)
select
  t.day_type,
  concat('typical_', t.day_type)       as label,
  t.mode,
  t.route_name,
  safe_cast(l.l.route_id as int64)     as route_id,
  l.l.board_seq                        as board_seq,
  t.board_key,
  l.l.board_code                       as board_code,
  t.board_name,
  l.l.alight_seq                       as alight_seq,
  t.alight_key,
  l.l.alight_code                      as alight_code,
  t.alight_name,
  t.avg_passengers                     as passengers,
  t.n_days,
  t.days_seen,
  w.window_start,
  w.window_end
from {{ ref('fct_od_typical') }} t
join latest l using (route_name, board_key, alight_key)
cross join win w
