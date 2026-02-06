with

subscription_events as (
    select * from {{ ref('int_subscription_events') }}
)

select
    event_id,
    subscription_id,
    user_id,
    product,
    plan_name,
    billing_period,
    event_type,
    event_date,
    mrr_amount,
    previous_mrr_amount,
    mrr_change
from subscription_events
