-- LTV prediction feature engineering
-- Prepares features for lifetime value prediction model

with

customers as (
    select * from {{ ref('dim_customers') }}
),

engagement as (
    select
        user_id,
        count(distinct activity_date) as total_active_days,
        sum(event_count) as total_events,
        avg(event_count) as avg_daily_events,
        max(activity_date) as last_active_date
    from {{ ref('fct_user_engagement_daily') }}
    group by 1
),

subscription_events as (
    select
        user_id,
        count(*) as total_sub_events,
        count(case when event_type = 'new' then 1 end) as new_count,
        count(case when event_type = 'cancel' then 1 end) as cancel_count,
        count(case when event_type = 'churn' then 1 end) as churn_count,
        sum(mrr_change) as total_mrr_change
    from {{ ref('fct_subscription_events') }}
    group by 1
),

revenue as (
    select
        s.user_id,
        sum(r.net_revenue) as total_revenue
    from {{ ref('fct_revenue') }} r
    inner join {{ ref('stg_app_db__subscriptions') }} s
        on r.stripe_subscription_id = s.stripe_subscription_id
    group by 1
)

select
    c.user_id,
    c.account_tier,
    c.company_size,
    c.industry,
    c.signup_source,
    c.total_subscriptions,
    c.active_subscriptions,
    c.current_mrr,
    date_diff('day', cast(c.created_at as date), current_date) as account_age_days,
    coalesce(e.total_active_days, 0) as total_active_days,
    coalesce(e.total_events, 0) as total_events,
    coalesce(e.avg_daily_events, 0) as avg_daily_events,
    coalesce(se.total_sub_events, 0) as total_sub_events,
    coalesce(se.new_count, 0) as subscription_starts,
    coalesce(se.cancel_count, 0) as subscription_cancels,
    coalesce(se.churn_count, 0) as subscription_churns,
    coalesce(rev.total_revenue, 0) as total_revenue,
    case
        when date_diff('day', cast(c.created_at as date), current_date) > 0
        then coalesce(rev.total_revenue, 0) / date_diff('day', cast(c.created_at as date), current_date) * 365
        else 0
    end as annualized_revenue
from customers c
left join engagement e on c.user_id = e.user_id
left join subscription_events se on c.user_id = se.user_id
left join revenue rev on c.user_id = rev.user_id
