-- A load that "succeeds" into an empty table must fail the build.
select 'fct_boarding is empty' as problem
from (select count(*) as n from {{ ref('fct_boarding') }})
where n = 0
