-- dim_stop must hold exactly the distinct stops in the source: none dropped
-- by a join, none added by a fan-out.
select src.n as source_stops, dim.n as dim_stops
from (select count(distinct stop_ars) as n from {{ ref('stg_hourly_boarding') }}) src,
     (select count(*) as n from {{ ref('dim_stop') }}) dim
where src.n != dim.n
