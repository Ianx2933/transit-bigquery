-- Per-month row count and passenger sums must equal the raw staging copy.
-- Joined on month only, never on stop, so a stop-master fan-out cannot
-- inflate either side.
with src as (
  select
    cast(use_ym as int64)     as service_ym,
    count(*)                  as n,
    sum(boarding_passengers)  as boardings,
    sum(alighting_passengers) as alightings
  from {{ source('staging', 'hourly_bus_stop_passenger') }}
  group by 1
),
fct as (
  select service_ym, count(*) as n, sum(boardings) as boardings, sum(alightings) as alightings
  from {{ ref('fct_boarding') }}
  group by 1
)
select
  coalesce(src.service_ym, fct.service_ym) as service_ym,
  src.n as src_rows, fct.n as fct_rows,
  src.boardings as src_boardings, fct.boardings as fct_boardings,
  src.alightings as src_alightings, fct.alightings as fct_alightings
from src
full outer join fct using (service_ym)
where src.n is null or fct.n is null
   or src.n != fct.n
   or abs(coalesce(src.boardings, 0)  - coalesce(fct.boardings, 0))  > 0.5
   or abs(coalesce(src.alightings, 0) - coalesce(fct.alightings, 0)) > 0.5
