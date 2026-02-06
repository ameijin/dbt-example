-- Subscriptions that are active should not have an ended_at date
select
    subscription_id,
    subscription_status,
    ended_at
from {{ ref('stg_app_db__subscriptions') }}
where subscription_status = 'active'
    and ended_at is not null
