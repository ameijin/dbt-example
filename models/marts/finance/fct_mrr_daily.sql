{{
    config(
        materialized='incremental',
        unique_key='mrr_daily_id',
        on_schema_change='append_new_columns'
    )
}}

with

daily_mrr_changes as (
    select * from {{ ref('int_daily_mrr_changes') }}
),

with_cumulative as (
    select
        {{ dbt_utils.generate_surrogate_key(['date_day', 'product']) }} as mrr_daily_id,
        date_day,
        product,
        new_mrr,
        expansion_mrr,
        contraction_mrr,
        churned_mrr,
        net_mrr_change,
        event_count,
        sum(net_mrr_change) over (
            partition by product
            order by date_day
            rows between unbounded preceding and current row
        ) as cumulative_mrr,
        current_timestamp as loaded_at
    from daily_mrr_changes
    {% if is_incremental() %}
    where date_day > (select max(date_day) from {{ this }})
    {% endif %}
)

select * from with_cumulative
