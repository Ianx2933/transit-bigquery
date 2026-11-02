-- Terrain stats for the active dataset only, so a second terrain run
-- added later cannot fan out the stop dimension.
select
  t.dataset_id,
  t.node_id,
  t.elev_m,
  t.slope_pct
from {{ source('staging', 'bus_stop_terrain_stats') }} t
join {{ source('staging', 'terrain_active_dataset') }} a
  on a.dataset_id = t.dataset_id
