with

source as (
    select * from {{ source('salesforce', 'opportunities') }}
),

renamed as (
    select
        id as opportunity_id,
        account_id,
        name as opportunity_name,
        stage_name,
        amount as opportunity_amount,
        probability,
        cast(close_date as date) as close_date,
        type as opportunity_type,
        lead_source,
        is_won,
        is_closed,
        owner_id,
        cast(created_date as timestamp) as created_at,
        cast(last_modified_date as timestamp) as last_modified_at,
        loaded_at
    from source
)

select * from renamed
