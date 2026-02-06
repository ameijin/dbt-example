with

charges as (
    select * from {{ ref('stg_stripe__charges') }}
),

invoices as (
    select * from {{ ref('stg_stripe__invoices') }}
),

subscriptions as (
    select * from {{ ref('stg_app_db__subscriptions') }}
),

-- Revenue from successful charges
charge_revenue as (
    select
        charge_id as revenue_id,
        'charge' as revenue_source,
        customer_id,
        subscription_id as stripe_subscription_id,
        amount_dollars as revenue_amount,
        amount_refunded_dollars as refund_amount,
        amount_dollars - amount_refunded_dollars as net_revenue,
        currency,
        charge_status as status,
        is_paid,
        created_at as revenue_date
    from charges
    where charge_status = 'succeeded'
),

-- Revenue from paid invoices
invoice_revenue as (
    select
        invoice_id as revenue_id,
        'invoice' as revenue_source,
        customer_id,
        subscription_id as stripe_subscription_id,
        total_dollars as revenue_amount,
        0 as refund_amount,
        amount_paid_dollars as net_revenue,
        currency,
        invoice_status as status,
        invoice_status = 'paid' as is_paid,
        created_at as revenue_date
    from invoices
    where invoice_status = 'paid'
),

-- Combine both revenue sources
combined as (
    select * from charge_revenue
    union all
    select * from invoice_revenue
),

-- Enrich with subscription product info
enriched as (
    select
        c.revenue_id,
        c.revenue_source,
        c.customer_id,
        c.stripe_subscription_id,
        s.product,
        s.plan_name,
        c.revenue_amount,
        c.refund_amount,
        c.net_revenue,
        c.currency,
        c.status,
        c.is_paid,
        c.revenue_date
    from combined c
    left join subscriptions s
        on c.stripe_subscription_id = s.stripe_subscription_id
)

select * from enriched
