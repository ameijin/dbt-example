with

subscriptions as (
    select * from {{ ref('stg_app_db__subscriptions') }}
),

users as (
    select * from {{ ref('stg_app_db__users') }}
),

channel_mapping as (
    select * from {{ ref('utm_channel_mapping') }}
),

-- Join subscriptions to users' UTM data, then map to channels
attributed as (
    select
        s.subscription_id,
        s.user_id,
        s.product,
        s.plan_name,
        s.monthly_amount,
        s.subscription_status,
        s.created_at as subscription_created_at,
        u.signup_source,
        u.utm_source,
        u.utm_medium,
        u.utm_campaign,
        coalesce(cm.channel, 'other') as acquisition_channel
    from subscriptions s
    inner join users u
        on s.user_id = u.user_id
    left join channel_mapping cm
        on coalesce(u.utm_source, '') = coalesce(cm.utm_source, '')
        and coalesce(u.utm_medium, '') = coalesce(cm.utm_medium, '')
)

select * from attributed
