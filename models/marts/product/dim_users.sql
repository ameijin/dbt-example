with

users as (
    select * from {{ ref('stg_app_db__users') }}
),

engagement_summary as (
    select
        user_id,
        count(distinct activity_date) as active_days,
        sum(event_count) as total_events,
        sum(session_count) as total_sessions,
        min(activity_date) as first_activity_date,
        max(activity_date) as last_activity_date
    from {{ ref('int_user_engagement_daily') }}
    group by 1
),

segment_identifies as (
    select
        user_id,
        max(event_timestamp) as last_identified_at
    from {{ ref('stg_segment__identifies') }}
    group by 1
)

select
    u.user_id,
    u.email,
    u.first_name,
    u.last_name,
    u.full_name,
    u.account_status,
    u.account_tier,
    u.company_name,
    u.company_size,
    u.industry,
    u.country_code,
    u.signup_source,
    u.created_at,
    u.last_login_at,
    u.is_active_user,
    coalesce(es.active_days, 0) as active_days,
    coalesce(es.total_events, 0) as total_events,
    coalesce(es.total_sessions, 0) as total_sessions,
    es.first_activity_date,
    es.last_activity_date,
    si.last_identified_at
from users u
left join engagement_summary es
    on u.user_id = es.user_id
left join segment_identifies si
    on u.user_id = si.user_id
