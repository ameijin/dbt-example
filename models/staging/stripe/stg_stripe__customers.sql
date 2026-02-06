with

source as (
    select * from {{ source('stripe', 'customers') }}
),

renamed as (
    select
        id as customer_id,
        lower(trim(email)) as email,
        name as customer_name,
        description as company_name,
        currency,
        default_payment_method,
        to_timestamp(created) as created_at,
        livemode as is_live,
        delinquent as is_delinquent,
        cast(metadata_user_id as integer) as user_id,
        loaded_at
    from source
)

select * from renamed
