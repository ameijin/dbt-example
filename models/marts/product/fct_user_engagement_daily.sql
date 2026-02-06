with

engagement as (
    select * from {{ ref('int_user_engagement_daily') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['user_id', 'product', 'activity_date']) }} as engagement_id,
    user_id,
    product,
    activity_date,
    event_count,
    distinct_event_types,
    session_count,
    total_duration_seconds
from engagement
