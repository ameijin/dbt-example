with

acquisition as (
    select * from {{ ref('int_user_acquisition_channels') }}
),

subscriptions as (
    select * from {{ ref('stg_app_db__subscriptions') }}
),

-- Get first subscription per user
first_subscription as (
    select
        user_id,
        min(created_at) as first_subscription_date,
        min_by(product, created_at) as first_product,
        min_by(plan_name, created_at) as first_plan,
        min_by(monthly_amount, created_at) as first_mrr
    from subscriptions
    group by 1
)

select
    a.user_id,
    a.email,
    a.acquisition_channel,
    a.signup_source,
    a.utm_source,
    a.utm_medium,
    a.utm_campaign,
    a.referral_code,
    a.signup_date,
    a.account_tier,
    a.account_status,
    fs.first_subscription_date,
    fs.first_product,
    fs.first_plan,
    coalesce(fs.first_mrr, 0) as first_mrr,
    fs.first_subscription_date is not null as has_converted,
    case
        when fs.first_subscription_date is not null
        then date_diff('day', cast(a.signup_date as date), cast(fs.first_subscription_date as date))
    end as days_to_conversion
from acquisition a
left join first_subscription fs
    on a.user_id = fs.user_id
