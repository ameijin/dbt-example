-- Churn cohort analysis: retention by monthly signup cohort
-- Shows what percentage of each cohort is still active N months later

with

customer_cohorts as (
    select
        user_id,
        date_trunc('month', cast(created_at as date)) as signup_cohort,
        current_mrr,
        active_subscriptions
    from {{ ref('dim_customers') }}
),

monthly_activity as (
    select
        user_id,
        date_trunc('month', activity_date) as activity_month
    from {{ ref('fct_user_engagement_daily') }}
    group by 1, 2
),

cohort_retention as (
    select
        cc.signup_cohort,
        ma.activity_month,
        date_diff('month', cc.signup_cohort, ma.activity_month) as months_since_signup,
        count(distinct cc.user_id) as active_users
    from customer_cohorts cc
    inner join monthly_activity ma
        on cc.user_id = ma.user_id
    where date_diff('month', cc.signup_cohort, ma.activity_month) >= 0
    group by 1, 2, 3
),

cohort_sizes as (
    select
        signup_cohort,
        count(*) as cohort_size
    from customer_cohorts
    group by 1
)

select
    cr.signup_cohort,
    cs.cohort_size,
    cr.months_since_signup,
    cr.active_users,
    round(cr.active_users * 100.0 / cs.cohort_size, 1) as retention_pct
from cohort_retention cr
inner join cohort_sizes cs
    on cr.signup_cohort = cs.signup_cohort
order by cr.signup_cohort, cr.months_since_signup
