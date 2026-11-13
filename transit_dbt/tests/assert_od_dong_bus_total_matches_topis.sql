{{ config(severity='warn') }}
-- Corrected bus stop OD and TOPIS's district bus OD come from the same smart-card
-- records, so their daily totals agree: within 0.2% on every day checked
-- (2026-11-13), the gap being the Han River ferry, which TOPIS counts as bus.
-- Returns days outside that. 2024-08-20 is a known TOPIS anomaly: its district
-- file has 144,008 more bus riders than its route file (-2.7%).
-- A warning, not an error: a TOPIS file should not block the warehouse build.
select service_date, ours_bus, topis_bus, total_diff
from {{ ref('od_dong_bus_check_daily') }}
where abs(total_diff) > 0.002
  and service_date != date '2024-08-20'
