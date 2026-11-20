-- Alighting reproduction check (R1 4-1). Hourly OD is the daily OD split by
-- the boarding stop's hourly profile, so boardings by hour match by design.
-- Alightings are the test: push every split rider forward by the ride time and
-- compare the hourly alightings this gives with the counted ones.
--
--   OD        typical-day bus OD of the twelve-month window, per day type
--             (od_typical_route_stop_serving), times the days of that type in
--             the window (dim_calendar)
--   split     hourly_profile per route, boarding stop and day type, else
--             default_hourly_ratio; boarding stops 'name:...' and '00000' are
--             left out (the API does the same)
--   ride      (alight_seq - board_seq) x m minutes, m = 0, 1.5, 2.0, 2.5 per
--             stop; boardings spread evenly over the hour, so a ride of t hours
--             puts 1-frac(t) of the riders floor(t) hours later and frac(t) one
--             hour after that; legs without an order are not moved
--   counted   fct_boarding (monthly hourly boardings and alightings per route
--             and stop, every day of the month) over the same months
--
-- The counts mix all day types; so does the prediction (days of each type).
-- Levels differ (OD correction, cards vs taps), so each route (or route and
-- stop) is scaled to the counted total first: the errors below are about the
-- shape over the day. WAPE = sum |predicted - counted| / sum counted.
--
-- Output, one table (section, variant, key, hour, predicted, counted, value):
--   summary  per variant: route_hour_wape, hour8_route_wape, stop_hour_wape,
--            routes, riders_unordered_share
--   seoul    the whole city's hourly shares, predicted and counted
--   route    route_hour_wape per route (variant 2.0 and board), for the worst
-- variant 'board' is the boarding control. ASCII only for PowerShell 5 piping.
with win as (
  select any_value(window_start) as d_from, any_value(window_end) as d_to,
         cast(format_date('%Y%m', any_value(window_start)) as int64) as ym_from,
         cast(format_date('%Y%m', any_value(window_end)) as int64) as ym_to
  from `project-9c77d932-f0d2-47bb-947.transit_marts.od_typical_route_stop_serving`
),
ndays as (
  select c.day_type, count(*) as n
  from `project-9c77d932-f0d2-47bb-947.transit_marts.dim_calendar` c, win
  where c.calendar_date between date_trunc(win.d_from, month) and last_day(win.d_to, month)
  group by 1
),
act_raw as (
  select trim(route_no) as route_no,
         case when regexp_contains(trim(stop_key), r'^\d+$') then lpad(trim(stop_key), 5, '0') else trim(stop_key) end as stop_key,
         hour, sum(boardings) as boardings, sum(alightings) as alightings
  from `project-9c77d932-f0d2-47bb-947.transit_marts.fct_boarding`, win
  where service_ym between win.ym_from and win.ym_to
  group by 1, 2, 3
),
od_raw as (
  select o.day_type, o.route_name as route_no,
         case when regexp_contains(o.board_key, r'^\d+$') then lpad(o.board_key, 5, '0') else o.board_key end as board_key,
         case when regexp_contains(o.alight_key, r'^\d+$') then lpad(o.alight_key, 5, '0') else o.alight_key end as alight_key,
         if(o.board_seq is not null and o.alight_seq > o.board_seq, o.alight_seq - o.board_seq, null) as stops_ridden,
         o.passengers * n.n as riders
  from `project-9c77d932-f0d2-47bb-947.transit_marts.od_typical_route_stop_serving` o
  join ndays n using (day_type)
  where o.mode = 'bus'
),
routes as (
  select distinct route_no from od_raw
  intersect distinct
  select distinct route_no from act_raw
),
od as (
  select o.* from od_raw o join routes using (route_no)
  where not starts_with(o.board_key, 'name:') and o.board_key != '00000'
),
act as (select a.* from act_raw a join routes using (route_no)),
prof as (
  select route_no, stop_key, day_type, hour, ratio
  from `project-9c77d932-f0d2-47bb-947.transit_marts.hourly_profile`
),
has_prof as (select distinct route_no, stop_key, day_type from prof),
dflt as (
  select hour, raw_ratio / sum(raw_ratio) over () as ratio
  from `project-9c77d932-f0d2-47bb-947.transit_marts.default_hourly_ratio`
),
split as (
  select o.route_no, o.board_key, o.alight_key, o.stops_ridden, p.hour, o.riders * p.ratio as r
  from od o join prof p
    on p.route_no = o.route_no and p.stop_key = o.board_key and p.day_type = o.day_type
  union all
  select o.route_no, o.board_key, o.alight_key, o.stops_ridden, d.hour, o.riders * d.ratio
  from od o cross join dflt d
  where not exists (select 1 from has_prof h
                    where h.route_no = o.route_no and h.stop_key = o.board_key and h.day_type = o.day_type)
),
variants as (select m from unnest([0.0, 1.5, 2.0, 2.5]) as m),
moved as (
  select s.route_no, s.alight_key as stop_key, format('%.1f', v.m) as variant,
         mod(s.hour + cast(floor(coalesce(s.stops_ridden, 0) * v.m / 60) as int64) + k, 24) as hour,
         s.r * if(k = 0,
                  1 - (coalesce(s.stops_ridden, 0) * v.m / 60 - floor(coalesce(s.stops_ridden, 0) * v.m / 60)),
                  coalesce(s.stops_ridden, 0) * v.m / 60 - floor(coalesce(s.stops_ridden, 0) * v.m / 60)) as r
  from split s cross join variants v cross join unnest([0, 1]) as k
),
pred as (
  select route_no, stop_key, variant, hour, sum(r) as p from moved where r > 0 group by 1, 2, 3, 4
  union all
  select route_no, board_key, 'board', hour, sum(r) from split group by 1, 2, 3, 4
),
cmp as (
  -- predicted and counted per route, stop, hour and variant (counted: alightings, or boardings for 'board')
  select coalesce(p.route_no, a.route_no) as route_no, coalesce(p.stop_key, a.stop_key) as stop_key,
         coalesce(p.variant, a.variant) as variant, coalesce(p.hour, a.hour) as hour,
         coalesce(p.p, 0) as p, coalesce(a.counted, 0) as a
  from pred p
  full outer join (
    select a.route_no, a.stop_key, a.hour, v.variant,
           if(v.variant = 'board', a.boardings, a.alightings) as counted
    from act a cross join (select distinct variant from pred) v
  ) a
    on p.variant = a.variant and p.route_no = a.route_no and p.stop_key = a.stop_key and p.hour = a.hour
),
cmp_clean as (select * from cmp where variant is not null),
route_h as (
  select route_no, variant, hour, sum(p) as p, sum(a) as a from cmp_clean group by 1, 2, 3
),
route_scaled as (
  select r.*, r.p * safe_divide(sum(r.a) over (partition by route_no, variant), sum(r.p) over (partition by route_no, variant)) as ps
  from route_h r
),
stop_scaled as (
  select c.*, c.p * safe_divide(sum(c.a) over (partition by route_no, stop_key, variant),
                                sum(c.p) over (partition by route_no, stop_key, variant)) as ps,
         sum(c.a) over (partition by route_no, stop_key, variant) as a_tot,
         sum(c.p) over (partition by route_no, stop_key, variant) as p_tot
  from cmp_clean c
),
summary as (
  select 'summary' as section, variant, 'route_hour_wape' as key, cast(null as int64) as hour,
         cast(null as float64) as predicted, cast(null as float64) as counted,
         sum(abs(ps - a)) / sum(a) as value
  from route_scaled where ps is not null group by variant
  union all
  select 'summary', variant, 'hour8_route_wape', null, null, null, sum(abs(ps - a)) / sum(a)
  from route_scaled where ps is not null and hour = 8 group by variant
  union all
  select 'summary', variant, 'stop_hour_wape', null, null, null, sum(abs(ps - a)) / sum(a)
  from stop_scaled where a_tot > 0 and p_tot > 0 group by variant
  union all
  select 'summary', variant, 'routes', null, null, null, count(distinct route_no)
  from route_scaled group by variant
  union all
  select 'summary', 'all', 'riders_unordered_share', null, null, null,
         sum(if(stops_ridden is null, riders, 0)) / sum(riders)
  from od
),
seoul as (
  select 'seoul' as section, variant, 'share' as key, hour,
         sum(p) / sum(sum(p)) over (partition by variant) as predicted,
         sum(a) / sum(sum(a)) over (partition by variant) as counted,
         cast(null as float64) as value
  from cmp_clean group by variant, hour
),
route_err as (
  select variant, route_no, sum(abs(ps - a)) / sum(a) as wape, sum(a) as counted
  from route_scaled where ps is not null and variant in ('2.0', 'board') group by 1, 2
),
route_rows as (
  select 'route' as section, variant, route_no as key, cast(null as int64) as hour,
         cast(null as float64) as predicted, counted, wape as value
  from route_err
  where counted >= 100000
  qualify row_number() over (partition by variant order by wape desc) <= 20
)
select * from summary
union all select * from seoul
union all select * from route_rows
order by section, variant, key, hour
