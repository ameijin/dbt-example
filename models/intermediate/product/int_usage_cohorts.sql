with

users as (
    select * from {{ ref('stg_app_db__users') }}
),

engagement as (
    select * from {{ ref('int_user_engagement_daily') }}
),

-- Assign users to signup cohorts (by month)
user_cohorts as (
    select
        user_id,
        date_trunc('month', cast(created_at as date)) as signup_cohort
    from users
),

-- Calculate activity per cohort period
cohort_activity as (
    select
        uc.signup_cohort,
        e.activity_date,
        date_diff('month', uc.signup_cohort, e.activity_date) as months_since_signup,
        count(distinct e.user_id) as active_users,
        sum(e.event_count) as total_events
    from user_cohorts uc
    inner join engagement e
        on uc.user_id = e.user_id
    group by 1, 2, 3
),

-- Get cohort sizes
cohort_sizes as (
    select
        signup_cohort,
        count(*) as cohort_size
    from user_cohorts
    group by 1
)

select
    ca.signup_cohort,
    ca.activity_date,
    ca.months_since_signup,
    cs.cohort_size,
    ca.active_users,
    round(ca.active_users * 100.0 / cs.cohort_size, 2) as retention_rate,
    ca.total_events
from cohort_activity ca
inner join cohort_sizes cs
    on ca.signup_cohort = cs.signup_cohort
where ca.months_since_signup >= 0
