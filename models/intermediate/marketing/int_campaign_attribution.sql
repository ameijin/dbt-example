with

acquisition as (
    select * from {{ ref('int_user_acquisition_channels') }}
),

subscriptions as (
    select * from {{ ref('stg_app_db__subscriptions') }}
),

-- Attribute subscription revenue to campaigns
campaign_revenue as (
    select
        a.utm_campaign,
        a.acquisition_channel,
        a.utm_source,
        a.utm_medium,
        count(distinct a.user_id) as users_acquired,
        count(distinct s.subscription_id) as subscriptions_created,
        sum(coalesce(s.monthly_amount, 0)) as total_mrr_generated,
        count(distinct case when s.is_active then s.subscription_id end) as active_subscriptions
    from acquisition a
    left join subscriptions s
        on a.user_id = s.user_id
    group by 1, 2, 3, 4
)

select * from campaign_revenue
