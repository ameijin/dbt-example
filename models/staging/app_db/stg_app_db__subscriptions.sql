with

source as (
    select * from {{ source('app_db', 'subscriptions') }}
),

renamed as (
    select
        id as subscription_id,
        user_id,
        plan_id,
        product,
        plan_name,
        billing_period,
        {{ cents_to_dollars('amount_cents') }} as subscription_amount,
        {{ cents_to_dollars('discount_cents') }} as discount_amount,
        lower(status) as subscription_status,
        quantity,
        trial_start_date,
        trial_end_date,
        current_period_start,
        current_period_end,
        cancel_at_period_end,
        canceled_at,
        ended_at,
        stripe_subscription_id,
        promo_code,
        payment_method,
        created_at,
        updated_at,
        loaded_at
    from source
    where deleted_at is null
),

with_calculated_fields as (
    select
        *,
        case
            when billing_period = 'annual' then round(subscription_amount / 12, 2)
            else subscription_amount
        end as monthly_amount,
        subscription_status in ('active', 'trial', 'past_due') as is_active,
        subscription_status = 'trial' as is_trial,
        subscription_status = 'canceled' as is_canceled
    from renamed
)

select * from with_calculated_fields
