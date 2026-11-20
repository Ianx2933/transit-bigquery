-- Every hourly profile has 24 hours and sums to one, so splitting a day's OD by
-- it keeps the day total (the 24 hours add up to the day). Returns keys that do not.
select route_no, stop_key, day_type, count(*) as hours, sum(ratio) as total
from {{ ref('hourly_profile') }}
group by 1, 2, 3
having count(*) != 24 or count(distinct hour) != 24 or abs(sum(ratio) - 1) > 1e-9
