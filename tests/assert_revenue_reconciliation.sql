-- Net revenue should never be negative for succeeded charges
select
    revenue_id,
    revenue_source,
    net_revenue
from {{ ref('fct_revenue') }}
where net_revenue < 0
    and status = 'succeeded'
