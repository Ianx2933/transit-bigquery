-- One row per corrected OD row. The raw date is YYMMDD and equals the partition
-- day (od_load.py checks this). Empty strings from the CSV become NULL.
-- 한강버스 is the only ferry route; the correction names it 한강버스01.
select
  parse_date('%y%m%d', service_date_raw)          as service_date,
  case when route_name like '한강버스%' then 'ferry' else 'bus' end as mode,
  route_name,
  nullif(trim(route_id), '')                      as route_id,
  board_seq,
  nullif(trim(board_ars), '')                     as board_ars,
  nullif(trim(board_code), '')                    as board_code,
  board_name,
  alight_seq,
  nullif(trim(alight_ars), '')                    as alight_ars,
  nullif(trim(alight_code), '')                   as alight_code,
  alight_name,
  passengers
from {{ source('staging', 'od_route_stop') }}
