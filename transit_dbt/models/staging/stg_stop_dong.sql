-- Stop standard code -> (gu, dong) from the active terrain snapshot's assignment,
-- named the way TOPIS names dongs. Only Seoul dongs get a gu name (seed seoul_gu);
-- stops outside Seoul have no row here (TOPIS calls them 서울시외).
with active as (
  select dataset_id from {{ source('staging', 'terrain_active_dataset') }}
)
select
  a.node_id                                           as stop_code,
  a.adm_cd,
  g.gu_name                                           as gu,
  regexp_replace(trim(a.adm_nm), r'[,·ㆍ・‧]', '·')   as dong
from {{ source('staging', 'terrain_stop_dong_assignment') }} a
join active using (dataset_id)
join {{ ref('seoul_gu') }} g on substr(a.adm_cd, 1, 5) = g.gu_code
