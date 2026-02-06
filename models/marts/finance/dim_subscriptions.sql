with

subscriptions as (
    select * from {{ ref('stg_app_db__subscriptions') }}
),

plan_catalog as (
    select * from {{ ref('plan_catalog') }}
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['s.subscription_id']) }} as subscription_key,
        s.subscription_id,
        s.user_id,
        s.product,
        s.plan_name,
        s.billing_period,
        s.subscription_amount,
        s.discount_amount,
        s.monthly_amount,
        s.subscription_status,
        {{ get_subscription_status(
            's.subscription_status',
            's.canceled_at',
            's.ended_at',
            "coalesce(cast(s.trial_end_date as date), cast('1900-01-01' as date))"
        ) }} as derived_status,
        s.quantity,
        s.stripe_subscription_id,
        s.promo_code,
        s.payment_method,
        s.trial_start_date,
        s.trial_end_date,
        s.current_period_start,
        s.current_period_end,
        s.canceled_at,
        s.ended_at,
        s.created_at,
        s.is_active,
        s.is_trial,
        s.is_canceled,
        pc.max_users as plan_max_users,
        pc.storage_gb as plan_storage_gb,
        pc.support_tier as plan_support_tier
    from subscriptions s
    left join plan_catalog pc
        on s.product = pc.product
        and s.plan_name = pc.plan_name
        and s.billing_period = pc.billing_period
)

select * from joined
