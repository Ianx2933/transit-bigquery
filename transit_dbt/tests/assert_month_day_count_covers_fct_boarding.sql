-- Every month with bus boardings has its day counts, or the demand map could
-- not split that month into day types. Rows returned = months missing.
select distinct f.service_ym
from {{ ref('fct_boarding') }} f
left join {{ ref('month_day_count') }} c using (service_ym)
where c.service_ym is null
