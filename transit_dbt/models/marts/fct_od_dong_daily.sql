{{ config(
    materialized='table',
    partition_by={'field': 'service_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['day_type', 'origin_cd', 'dest_cd']
) }}
-- TOPIS district mode OD per day with the codes Cloud SQL's od_dong_typical
-- uses, so the API can average any chosen months the way od_dong_typical
-- averages the last twelve. Codes come from stg_stop_dong exactly as in
-- od/od_dong_export.py: a (gu, dong) without a Seoul code, and 서울시외, becomes
-- 'outside'. Clustered by origin and destination code: a period query for one
-- area reads only that area's blocks of the chosen days.
with codes as (
  select gu, dong, any_value(adm_cd) as adm_cd
  from {{ ref('stg_stop_dong') }}
  group by gu, dong
)
select
  o.service_date,
  c.day_type,
  coalesce(oc.adm_cd, 'outside')                                as origin_cd,
  any_value(if(oc.adm_cd is null, 'outside', o.origin_dong))    as origin_name,
  coalesce(dc.adm_cd, 'outside')                                as dest_cd,
  any_value(if(dc.adm_cd is null, 'outside', o.dest_dong))      as dest_name,
  sum(ifnull(o.subway_passengers, 0))                           as subway_rides,
  sum(ifnull(o.bus_passengers, 0))                              as bus_rides
from {{ ref('stg_od_dong_mode') }} o
join {{ ref('dim_calendar') }} c on c.calendar_date = o.service_date
left join codes oc on oc.gu = o.origin_gu and oc.dong = o.origin_dong
left join codes dc on dc.gu = o.dest_gu and dc.dong = o.dest_dong
group by 1, 2, 3, 5
