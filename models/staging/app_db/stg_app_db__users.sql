{{ config (materialized='view') }}

/*
 Staging model for users

 Transformations:
 - Standardize email to lowercase
 - Combine first/last name
 - filter out deleted users and test accounts

 */

with
    source as (select
                   *
               from
                   {{ source('app_db', 'users') }})
  , renamed as (select
                    -- IDs
                    id                                                           as user_id
                  ,

                    -- Personal Info
                    lower(trim(email))                                           as email
                  , lower(trim(first_name))                                      as first_name
                  , lower(trim(last_name))                                       as last_name
                  , concat(lower(trim(first_name)), ' ', lower(trim(last_name))) as full_name
                  ,

                    -- Account info
                    lower(account_status)                                        as account_status
                  , company_name
                  ,

                    -- Timestamps
                    created_at
                  , updated_at
                  , loaded_at
                  ,

                    -- Derived flags
                    account_status = 'active'                                    as is_active_user
                  , email like '%@test.com'                                      as is_test_account

                from
                    source
                where
                    deleted_at is null)

select
    *
from
    renamed
where
    is_test_account = false