{{ config(materialized='table') }}
-- One row per OD day: corrected bus stop OD summed to dongs against TOPIS's
-- district bus OD.
--   total_diff   ours / topis - 1 over all pairs (서울시외 included)
--   wape         sum |ours - topis| / sum topis, all pairs
--   wape_seoul   the same over pairs with both dongs in Seoul: how differently
--                the two sides place stops in dongs
--   *_only       pairs present on one side only, and their passengers
select
  service_date,
  sum(ours_bus)                                                                as ours_bus,
  sum(topis_bus)                                                               as topis_bus,
  round(safe_divide(sum(ours_bus), sum(topis_bus)) - 1, 4)                     as total_diff,
  round(safe_divide(sum(abs(coalesce(ours_bus, 0) - coalesce(topis_bus, 0))), sum(topis_bus)), 4) as wape,
  round(safe_divide(sum(if(both_in_seoul, abs(coalesce(ours_bus, 0) - coalesce(topis_bus, 0)), 0)),
                    sum(if(both_in_seoul, topis_bus, 0))), 4)                  as wape_seoul,
  round(safe_divide(sum(if(origin_gu = '서울시외' or dest_gu = '서울시외', topis_bus, 0)),
                    sum(topis_bus)), 4)                                        as topis_outside_share,
  round(safe_divide(sum(if(origin_gu = '서울시외' or dest_gu = '서울시외', ours_bus, 0)),
                    sum(ours_bus)), 4)                                         as ours_outside_share,
  countif(ours_bus is not null and topis_bus is not null)                      as pairs_both,
  countif(topis_bus is null)                                                   as pairs_ours_only,
  sum(if(topis_bus is null, ours_bus, 0))                                      as ours_only_bus,
  countif(ours_bus is null)                                                    as pairs_topis_only,
  sum(if(ours_bus is null, topis_bus, 0))                                      as topis_only_bus
from {{ ref('fct_od_dong_bus_check') }}
group by 1
