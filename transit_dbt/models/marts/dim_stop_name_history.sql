{{ config(materialized='table') }}

-- Every name each stop has carried, with the months it was observed.
-- Compared without the route ordinal suffix, so only real renames count.
-- Built from the monthly data itself rather than a dbt snapshot: a snapshot
-- only records changes between future runs, while this history is already
-- in the source months.
select
  stop_ars                         as stop_key,
  stop_base_name                   as stop_name,
  min(service_ym)                  as first_seen_ym,
  max(service_ym)                  as last_seen_ym,
  count(distinct service_ym)       as months_seen,
  sum(boarding_passengers)         as boardings
from {{ ref('stg_hourly_boarding') }}
group by 1, 2
