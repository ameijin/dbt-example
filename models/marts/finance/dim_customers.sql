with

users as (
    select * from {{ ref('stg_app_db__users') }}
),

stripe_customers as (
    select * from {{ ref('stg_stripe__customers') }}
),

subscriptions as (
    select * from {{ ref('stg_app_db__subscriptions') }}
),

-- Aggregate subscription info per user
subscription_summary as (
    select
        user_id,
        count(*) as total_subscriptions,
        count(case when is_active then 1 end) as active_subscriptions,
        sum(case when is_active then monthly_amount else 0 end) as current_mrr,
        min(created_at) as first_subscription_at,
        max(created_at) as last_subscription_at
    from subscriptions
    group by 1
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['u.user_id']) }} as customer_key,
        u.user_id,
        u.email,
        u.first_name,
        u.last_name,
        u.full_name,
        u.account_status,
        u.account_tier,
        u.company_name,
        u.company_size,
        u.industry,
        u.country_code,
        u.signup_source,
        u.utm_source,
        u.utm_medium,
        u.utm_campaign,
        sc.customer_id as stripe_customer_id,
        sc.is_delinquent as is_stripe_delinquent,
        coalesce(ss.total_subscriptions, 0) as total_subscriptions,
        coalesce(ss.active_subscriptions, 0) as active_subscriptions,
        coalesce(ss.current_mrr, 0) as current_mrr,
        ss.first_subscription_at,
        ss.last_subscription_at,
        u.created_at,
        u.last_login_at,
        u.is_active_user
    from users u
    left join stripe_customers sc
        on u.user_id = sc.user_id
    left join subscription_summary ss
        on u.user_id = ss.user_id
)

select * from joined
