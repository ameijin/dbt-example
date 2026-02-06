{% snapshot user_plan_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='user_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}

select
    user_id,
    account_status,
    account_tier,
    is_active_user,
    cast(updated_at as timestamp) as updated_at
from {{ ref('stg_app_db__users') }}

{% endsnapshot %}
