-- MRR should never be negative at the cumulative level
select
    mrr_daily_id,
    date_day,
    product,
    cumulative_mrr
from {{ ref('fct_mrr_daily') }}
where cumulative_mrr < 0
