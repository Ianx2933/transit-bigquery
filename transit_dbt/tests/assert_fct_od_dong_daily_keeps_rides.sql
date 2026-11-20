-- Attaching codes and folding unknown districts into 'outside' must not lose or
-- add rides: per day, subway and bus rides equal the TOPIS rows they come from.
with ours as (
  select service_date, sum(subway_rides) as subway, sum(bus_rides) as bus
  from {{ ref('fct_od_dong_daily') }} group by 1
),
src as (
  select service_date, sum(ifnull(subway_passengers, 0)) as subway, sum(ifnull(bus_passengers, 0)) as bus
  from {{ ref('stg_od_dong_mode') }} group by 1
)
select s.service_date, s.subway, o.subway as ours_subway, s.bus, o.bus as ours_bus
from src s
left join ours o using (service_date)
where o.service_date is null
   or abs(s.subway - o.subway) > 1e-6 * greatest(1, s.subway)
   or abs(s.bus - o.bus) > 1e-6 * greatest(1, s.bus)
