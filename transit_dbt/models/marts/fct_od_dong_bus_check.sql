{{ config(
    materialized='table',
    partition_by={'field': 'service_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['origin_gu', 'origin_dong']
) }}
-- Corrected bus stop OD summed to dong pairs, next to TOPIS's own district bus OD
-- for the same day. Both count rides, not trips. A pair missing on one side has
-- NULL there. Stops without a Seoul dong go to 서울시외 / 서울시외, as TOPIS does,
-- so daily totals can be compared too. TOPIS can list a pair twice on a day, so
-- both sides are summed per pair before the join.
-- TOPIS counts the Han River ferry as bus; ours is mode = 'bus' only, so from
-- 2025-09 TOPIS is higher by the ferry's riders.
with ours as (
  select
    f.service_date,
    coalesce(b.gu, '서울시외')   as origin_gu, coalesce(b.dong, '서울시외') as origin_dong,
    coalesce(a.gu, '서울시외')   as dest_gu,   coalesce(a.dong, '서울시외') as dest_dong,
    sum(f.passengers)            as ours_bus
  from {{ ref('fct_od_stop') }} f
  left join {{ ref('stg_stop_dong') }} b on f.board_code = b.stop_code
  left join {{ ref('stg_stop_dong') }} a on f.alight_code = a.stop_code
  where f.mode = 'bus'
  group by 1, 2, 3, 4, 5
),
days as (
  select distinct service_date from {{ ref('fct_od_stop') }}
),
topis as (
  select service_date, origin_gu, origin_dong, dest_gu, dest_dong, sum(bus_passengers) as topis_bus
  from {{ ref('stg_od_dong_mode') }}
  join days using (service_date)
  where bus_passengers > 0
  group by 1, 2, 3, 4, 5
)
select
  service_date, origin_gu, origin_dong, dest_gu, dest_dong,
  ours.ours_bus,
  topis.topis_bus,
  origin_gu != '서울시외' and dest_gu != '서울시외' as both_in_seoul
from ours
full outer join topis using (service_date, origin_gu, origin_dong, dest_gu, dest_dong)
