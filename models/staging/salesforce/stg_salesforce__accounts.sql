with

source as (
    select * from {{ source('salesforce', 'accounts') }}
),

renamed as (
    select
        id as account_id,
        name as account_name,
        type as account_type,
        industry,
        annual_revenue,
        number_of_employees,
        billing_city,
        billing_country,
        owner_id,
        cast(created_date as timestamp) as created_at,
        cast(last_modified_date as timestamp) as last_modified_at,
        is_deleted,
        loaded_at
    from source
    where not is_deleted
)

select * from renamed
