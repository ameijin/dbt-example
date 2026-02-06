with

usage_events as (
    select * from {{ ref('stg_app_db__usage_events') }}
),

segment_tracks as (
    select * from {{ ref('stg_segment__tracks') }}
),

-- Combine app_db events with segment tracks
app_events as (
    select
        event_id,
        user_id,
        'app_db' as event_source,
        event_type as event_name,
        product,
        event_timestamp,
        session_id,
        duration_seconds
    from usage_events
),

segment_events as (
    select
        track_id as event_id,
        user_id,
        'segment' as event_source,
        event_name,
        null as product,
        event_timestamp,
        null as session_id,
        null as duration_seconds
    from segment_tracks
)

select * from app_events
union all
select * from segment_events
