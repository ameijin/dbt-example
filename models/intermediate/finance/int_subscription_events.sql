with

subscriptions as (
    select * from {{ ref('stg_app_db__subscriptions') }}
),

-- Generate subscription lifecycle events from subscription state
events as (
    -- New subscription event
    select
        subscription_id,
        user_id,
        product,
        plan_name,
        billing_period,
        'new' as event_type,
        created_at as event_date,
        monthly_amount as mrr_amount,
        0 as previous_mrr_amount,
        monthly_amount as mrr_change
    from subscriptions

    union all

    -- Cancellation event
    select
        subscription_id,
        user_id,
        product,
        plan_name,
        billing_period,
        'cancel' as event_type,
        canceled_at as event_date,
        0 as mrr_amount,
        monthly_amount as previous_mrr_amount,
        -monthly_amount as mrr_change
    from subscriptions
    where canceled_at is not null

    union all

    -- Expiry/churn event
    select
        subscription_id,
        user_id,
        product,
        plan_name,
        billing_period,
        'churn' as event_type,
        ended_at as event_date,
        0 as mrr_amount,
        monthly_amount as previous_mrr_amount,
        -monthly_amount as mrr_change
    from subscriptions
    where ended_at is not null
        and canceled_at is null
)

select
    {{ dbt_utils.generate_surrogate_key(['subscription_id', 'event_type', 'event_date']) }} as event_id,
    *
from events
where event_date is not null
