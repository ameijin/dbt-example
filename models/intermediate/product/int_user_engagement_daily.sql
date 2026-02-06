with

events as (
    select * from {{ ref('stg_app_db__usage_events') }}
),

daily_engagement as (
    select
        user_id,
        product,
        cast(event_timestamp as date) as activity_date,
        count(*) as event_count,
        count(distinct event_type) as distinct_event_types,
        count(distinct session_id) as session_count,
        sum(coalesce(duration_seconds, 0)) as total_duration_seconds
    from events
    group by 1, 2, 3
)

select * from daily_engagement
