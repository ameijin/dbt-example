with

source as (
    select * from {{ source('app_db', 'usage_events') }}
),

renamed as (
    select
        id as event_id,
        user_id,
        event_type,
        product,
        cast(event_timestamp as timestamp) as event_timestamp,
        session_id,
        page_url,
        feature_name,
        duration_seconds,
        properties,
        loaded_at
    from source
)

select * from renamed
