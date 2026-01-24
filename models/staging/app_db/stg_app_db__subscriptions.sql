{{ config(materialized='view') }}

/*
 Subscription staging model

 Business rules:
 - Prices stored in cents, convert to dollars
 - Annual plans: divide by 12 to get monthly equivalent
 - Exclude deleted subscriptions
 - Add helpful flags (is_active, is_trial, etc.)
 */

with
    source as (select
                   *
               from
                   {{ source('app_db', 'subscriptions') }})
  , renamed as (select
                    -- IDs
                    id                                     as subscription_id
                  , user_id
                  ,

                    -- Product details
                    product
                  , plan_name
                  , billing_period
                  ,

                    -- Pricing (convert cents to dollars)
                    {{ cents_to_dollars('amount_cents') }} as subscription_amount
                  ,

                    -- Status
                    lower(status)                          as subscription_status
                  ,

                    -- Timestamps
                    created_at
                from source
                where deleted_at is null
                  ),
    with_calculated_fields as (
        select
            *,

            -- Calculate monthly amount (normalize annual to monthly)
        case
            when billing_period = 'annual' then subscription_amount / 12
        else subscription_amount
        end as monthly_amount,

            -- Helpful boolean flags
        subscription_status in ('active', 'trial', 'past_due') as is_active,
        subscription_status = 'trial' as is_trial,
        subscription_status = 'canceled' as is_canceled
        from renamed
    )

select * from with_calculated_fields