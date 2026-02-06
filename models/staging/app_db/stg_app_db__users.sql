with

source as (
    select * from {{ source('app_db', 'users') }}
),

renamed as (
    select
        id as user_id,
        lower(trim(email)) as email,
        lower(trim(first_name)) as first_name,
        lower(trim(last_name)) as last_name,
        concat(lower(trim(first_name)), ' ', lower(trim(last_name))) as full_name,
        lower(account_status) as account_status,
        account_tier,
        company_name,
        company_size,
        industry,
        country_code,
        signup_source,
        utm_source,
        utm_medium,
        utm_campaign,
        referral_code,
        uses_cloud_sync,
        uses_team_chat,
        uses_data_hub,
        created_at,
        updated_at,
        last_login_at,
        trial_started_at,
        trial_ended_at,
        loaded_at,
        account_status = 'active' as is_active_user,
        coalesce(is_test_user, false) as is_test_user,
        coalesce(is_internal_user, false) as is_internal_user
    from source
    where deleted_at is null
)

select *
from renamed
where not is_test_user
