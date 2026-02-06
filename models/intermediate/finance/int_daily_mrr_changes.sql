with

date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('start_date') ~ "' as date)",
        end_date="current_date"
    ) }}
),

dates as (
    select cast(date_day as date) as date_day
    from date_spine
),

subscription_events as (
    select * from {{ ref('int_subscription_events') }}
),

-- Aggregate MRR changes by day and event type
daily_changes as (
    select
        cast(event_date as date) as date_day,
        product,
        sum(case when event_type = 'new' then mrr_change else 0 end) as new_mrr,
        sum(case when event_type = 'upgrade' then mrr_change else 0 end) as expansion_mrr,
        sum(case when event_type = 'downgrade' then mrr_change else 0 end) as contraction_mrr,
        sum(case when event_type in ('cancel', 'churn') then mrr_change else 0 end) as churned_mrr,
        sum(mrr_change) as net_mrr_change,
        count(*) as event_count
    from subscription_events
    group by 1, 2
),

-- Cross join dates with products for gap-free series
products as (
    select distinct product from {{ ref('stg_app_db__subscriptions') }}
),

date_product_spine as (
    select
        d.date_day,
        p.product
    from dates d
    cross join products p
),

-- Fill gaps with zeros
filled as (
    select
        dp.date_day,
        dp.product,
        coalesce(dc.new_mrr, 0) as new_mrr,
        coalesce(dc.expansion_mrr, 0) as expansion_mrr,
        coalesce(dc.contraction_mrr, 0) as contraction_mrr,
        coalesce(dc.churned_mrr, 0) as churned_mrr,
        coalesce(dc.net_mrr_change, 0) as net_mrr_change,
        coalesce(dc.event_count, 0) as event_count
    from date_product_spine dp
    left join daily_changes dc
        on dp.date_day = dc.date_day
        and dp.product = dc.product
)

select * from filled
