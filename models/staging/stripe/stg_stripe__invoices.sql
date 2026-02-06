with

source as (
    select * from {{ source('stripe', 'invoices') }}
),

renamed as (
    select
        id as invoice_id,
        customer_id,
        subscription_id,
        status as invoice_status,
        currency,
        {{ cents_to_dollars('amount_due') }} as amount_due_dollars,
        {{ cents_to_dollars('amount_paid') }} as amount_paid_dollars,
        {{ cents_to_dollars('amount_remaining') }} as amount_remaining_dollars,
        {{ cents_to_dollars('subtotal') }} as subtotal_dollars,
        {{ cents_to_dollars('tax') }} as tax_dollars,
        {{ cents_to_dollars('total') }} as total_dollars,
        to_timestamp(period_start) as period_start_at,
        to_timestamp(period_end) as period_end_at,
        to_timestamp(due_date) as due_at,
        to_timestamp(created) as created_at,
        loaded_at
    from source
)

select * from renamed
