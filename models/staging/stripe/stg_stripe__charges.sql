with

source as (
    select * from {{ source('stripe', 'charges') }}
),

renamed as (
    select
        id as charge_id,
        customer_id,
        subscription_id,
        {{ cents_to_dollars('amount') }} as amount_dollars,
        {{ cents_to_dollars('amount_refunded') }} as amount_refunded_dollars,
        currency,
        status as charge_status,
        paid as is_paid,
        failure_code,
        failure_message,
        to_timestamp(created) as created_at,
        loaded_at
    from source
)

select * from renamed
