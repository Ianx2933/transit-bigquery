{{ config(materialized='table') }}
-- Days of each day type in each month (Sunday and public holidays together),
-- as the Cloud SQL pipeline counts them for solve_dow_hourly_ratio. The API
-- reads it to split a month's bus boardings into day types
-- (dow_boarding_shape) and to list the months the demand map can show.
select
  cast(regexp_replace(use_ym, r'[^0-9]', '') as int64) as service_ym,
  mon, tue, wed, thu, fri, sat, sun_holiday
from {{ source('staging', 'month_day_count') }}
