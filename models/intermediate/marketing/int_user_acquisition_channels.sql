with

users as (
    select * from {{ ref('stg_app_db__users') }}
),

channel_mapping as (
    select * from {{ ref('utm_channel_mapping') }}
),

attributed as (
    select
        u.user_id,
        u.email,
        u.signup_source,
        u.utm_source,
        u.utm_medium,
        u.utm_campaign,
        u.referral_code,
        coalesce(cm.channel, 'other') as acquisition_channel,
        u.created_at as signup_date,
        u.account_tier,
        u.account_status
    from users u
    left join channel_mapping cm
        on coalesce(u.utm_source, '') = coalesce(cm.utm_source, '')
        and coalesce(u.utm_medium, '') = coalesce(cm.utm_medium, '')
)

select * from attributed
