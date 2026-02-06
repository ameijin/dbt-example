with

source as (
    select * from {{ source('segment', 'identifies') }}
),

renamed as (
    select
        id as identify_id,
        cast(user_id as integer) as user_id,
        anonymous_id,
        cast("timestamp" as timestamp) as event_timestamp,
        cast(received_at as timestamp) as received_at,
        lower(trim(email)) as email,
        name as display_name,
        company_name,
        plan as current_plan,
        context_ip,
        loaded_at
    from source
)

select * from renamed
