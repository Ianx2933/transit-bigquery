{{ config(
    materialized='table',
    partition_by={'field': 'service_ym', 'data_type': 'int64',
                  'range': {'start': 202401, 'end': 202701, 'interval': 1}},
    cluster_by=['route_no', 'stop_key']
) }}
select
  service_ym,
  service_month,
  hour,
  route_no,
  stop_ars                                      as stop_key,
  cast(round(boarding_passengers) as int64)     as boardings,
  cast(round(alighting_passengers) as int64)    as alightings
from {{ ref('stg_hourly_boarding') }}
