with

source as (
    select * from {{ source('segment', 'tracks') }}
),

renamed as (
    select
        id as track_id,
        cast(user_id as integer) as user_id,
        anonymous_id,
        event as event_name,
        cast("timestamp" as timestamp) as event_timestamp,
        cast(received_at as timestamp) as received_at,
        context_page_url,
        context_user_agent,
        context_ip,
        context_locale,
        loaded_at
    from source
)

select * from renamed
