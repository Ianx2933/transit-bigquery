{{ config(
    materialized='table',
    cluster_by=['route_name', 'service_date']
) }}
-- fct_od_stop again, clustered on route_name first and not partitioned, for the
-- API's period queries filtered on route_name (PeriodOdRepository).
-- fct_od_stop's day partitions are small (one TOPIS day, about 50 MB), so
-- clustering inside them prunes nothing and every query read whole days:
-- about 550 MB a route query in November 2026. Clustered over the whole table,
-- a query reads only the blocks of its keys (some MB). Columns the API reads only.
select
  service_date,
  mode,
  route_name,
  board_stop_key,
  alight_stop_key,
  board_name,
  alight_name,
  board_seq,
  alight_seq,
  passengers
from {{ ref('fct_od_stop') }}
