with

feature_adoption as (
    select * from {{ ref('int_feature_adoption') }}
),

users as (
    select * from {{ ref('stg_app_db__users') }}
),

product_features as (
    select * from {{ ref('product_features') }}
),

-- Count adopters per feature
adoption_stats as (
    select
        fa.product,
        fa.feature_name,
        count(distinct fa.user_id) as adopters,
        sum(fa.usage_count) as total_usage,
        avg(fa.usage_count) as avg_usage_per_user
    from feature_adoption fa
    group by 1, 2
),

-- Total users per product
product_user_counts as (
    select
        'cloudsync' as product,
        count(*) as total_users
    from users where uses_cloud_sync

    union all

    select
        'teamchat' as product,
        count(*) as total_users
    from users where uses_team_chat

    union all

    select
        'datahub' as product,
        count(*) as total_users
    from users where uses_data_hub
)

select
    as_tbl.product,
    as_tbl.feature_name,
    as_tbl.adopters,
    puc.total_users as product_users,
    round(as_tbl.adopters * 100.0 / nullif(puc.total_users, 0), 2) as adoption_rate_pct,
    as_tbl.total_usage,
    round(as_tbl.avg_usage_per_user, 1) as avg_usage_per_user,
    pf.starter_enabled,
    pf.professional_enabled,
    pf.enterprise_enabled
from adoption_stats as_tbl
left join product_user_counts puc
    on as_tbl.product = puc.product
left join product_features pf
    on as_tbl.product = pf.product
    and as_tbl.feature_name = pf.feature_name
