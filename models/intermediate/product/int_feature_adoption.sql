with

events as (
    select * from {{ ref('stg_app_db__usage_events') }}
),

feature_events as (
    select
        user_id,
        product,
        feature_name,
        min(event_timestamp) as first_used_at,
        max(event_timestamp) as last_used_at,
        count(*) as usage_count
    from events
    where event_type = 'feature_used'
        and feature_name is not null
    group by 1, 2, 3
)

select * from feature_events
