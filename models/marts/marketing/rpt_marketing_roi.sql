with

campaign_attribution as (
    select * from {{ ref('int_campaign_attribution') }}
),

customer_acquisition as (
    select * from {{ ref('fct_customer_acquisition') }}
),

-- Channel-level aggregation
channel_summary as (
    select
        acquisition_channel,
        count(distinct user_id) as total_signups,
        count(distinct case when has_converted then user_id end) as converted_users,
        round(
            count(distinct case when has_converted then user_id end) * 100.0
            / nullif(count(distinct user_id), 0), 2
        ) as conversion_rate_pct,
        sum(first_mrr) as total_mrr_acquired,
        round(avg(case when has_converted then first_mrr end), 2) as avg_mrr_per_customer,
        round(avg(case when has_converted then days_to_conversion end), 1) as avg_days_to_conversion
    from customer_acquisition
    group by 1
),

-- Campaign-level detail
campaign_summary as (
    select
        utm_campaign,
        acquisition_channel,
        utm_source,
        utm_medium,
        users_acquired,
        subscriptions_created,
        active_subscriptions,
        total_mrr_generated,
        round(total_mrr_generated / nullif(users_acquired, 0), 2) as mrr_per_user
    from campaign_attribution
)

select
    cs.utm_campaign,
    cs.acquisition_channel,
    cs.utm_source,
    cs.utm_medium,
    cs.users_acquired,
    cs.subscriptions_created,
    cs.active_subscriptions,
    cs.total_mrr_generated,
    cs.mrr_per_user,
    chs.conversion_rate_pct as channel_conversion_rate,
    chs.avg_days_to_conversion as channel_avg_days_to_convert
from campaign_summary cs
left join channel_summary chs
    on cs.acquisition_channel = chs.acquisition_channel
