{% snapshot subscription_pricing_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='subscription_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}

select
    subscription_id,
    user_id,
    product,
    plan_name,
    billing_period,
    subscription_amount,
    monthly_amount,
    subscription_status,
    cast(updated_at as timestamp) as updated_at
from {{ ref('stg_app_db__subscriptions') }}

{% endsnapshot %}
