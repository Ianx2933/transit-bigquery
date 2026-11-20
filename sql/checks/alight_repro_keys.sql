-- Step 1 of the alighting reproduction check (R1 4-1): do the keys meet?
-- Typical-day OD (od_typical_route_stop_serving, bus) against monthly hourly
-- counts (fct_boarding) over the same twelve months. Small: run it first.
-- ASCII only, so PowerShell 5 can pipe it to bq.
with od as (
  select route_name,
         case when regexp_contains(board_key, r'^\d+$') then lpad(board_key, 5, '0') else board_key end as board_key,
         case when regexp_contains(alight_key, r'^\d+$') then lpad(alight_key, 5, '0') else alight_key end as alight_key,
         board_seq, alight_seq, passengers
  from `project-9c77d932-f0d2-47bb-947.transit_marts.od_typical_route_stop_serving`
  where mode = 'bus' and day_type = 'weekday'
),
win as (
  select distinct cast(format_date('%Y%m', window_start) as int64) as ym_from,
                  cast(format_date('%Y%m', window_end) as int64) as ym_to
  from `project-9c77d932-f0d2-47bb-947.transit_marts.od_typical_route_stop_serving`
),
act as (
  select trim(route_no) as route_no,
         case when regexp_contains(trim(stop_key), r'^\d+$') then lpad(trim(stop_key), 5, '0') else trim(stop_key) end as stop_key,
         sum(boardings) as boardings, sum(alightings) as alightings
  from `project-9c77d932-f0d2-47bb-947.transit_marts.fct_boarding`, win
  where service_ym between win.ym_from and win.ym_to
  group by 1, 2
),
routes as (select distinct route_no from act)
select
  (select any_value(format('%d..%d', ym_from, ym_to)) from win)                                as months,
  (select count(distinct service_ym) from `project-9c77d932-f0d2-47bb-947.transit_marts.fct_boarding`, win
     where service_ym between win.ym_from and win.ym_to)                                     as months_with_counts,
  sum(passengers)                                                                             as od_weekday_riders,
  sum(if(r.route_no is not null, passengers, 0)) / sum(passengers)                            as share_route_found,
  sum(if(b.stop_key is not null, passengers, 0)) / sum(passengers)                            as share_board_found,
  sum(if(a.stop_key is not null, passengers, 0)) / sum(passengers)                            as share_alight_found,
  sum(if(od.board_seq is not null and od.alight_seq > od.board_seq, passengers, 0)) / sum(passengers) as share_ordered,
  approx_quantiles(if(od.alight_seq > od.board_seq, od.alight_seq - od.board_seq, null), 4)  as stops_ridden_quartiles
from od
left join routes r on r.route_no = od.route_name
left join act b on b.route_no = od.route_name and b.stop_key = od.board_key
left join act a on a.route_no = od.route_name and a.stop_key = od.alight_key
