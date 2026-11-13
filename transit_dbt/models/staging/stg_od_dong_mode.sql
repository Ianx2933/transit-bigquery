-- TOPIS district mode OD. Dongs are identified by (gu, dong) names, as in the source.
-- Dong names are normalized with the same rule as stg_stop_dong: TOPIS writes
-- 종로1,2,3,4가동 in some months, the boundary file 종로1·2·3·4가동.
-- TOPIS puts every stop outside Seoul in one pseudo dong, 서울시외 / 서울시외.
select
  service_date,
  trim(origin_gu)                                          as origin_gu,
  regexp_replace(trim(origin_dong), r'[,·ㆍ・‧]', '·')     as origin_dong,
  trim(dest_gu)                                            as dest_gu,
  regexp_replace(trim(dest_dong), r'[,·ㆍ・‧]', '·')       as dest_dong,
  total_passengers,
  subway_passengers,
  bus_passengers
from {{ source('staging', 'od_dong_mode') }}
