select
  node_id,
  stop_no         as stop_ars,
  stop_name       as master_stop_name,
  lng             as lon,
  lat,
  standard_code_flag
from {{ source('staging', 'bus_stop_location') }}
