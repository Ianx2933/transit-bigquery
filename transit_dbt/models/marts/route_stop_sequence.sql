{{ config(materialized='table') }}

-- Position of each stop along each route, parsed from the source name suffix.
-- A route can pass the same stop twice (outbound and at the turnaround), so
-- one (route_no, stop_key) pair may have more than one route_stop_seq.
-- Routes are re-sequenced over time; first/last month bound each ordinal.
select
  route_no,
  stop_ars                         as stop_key,
  route_stop_seq,
  min(service_ym)                  as first_seen_ym,
  max(service_ym)                  as last_seen_ym,
  count(distinct service_ym)       as months_seen
from {{ ref('stg_hourly_boarding') }}
where route_stop_seq is not null
group by 1, 2, 3
