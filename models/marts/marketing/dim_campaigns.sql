with

users as (
    select * from {{ ref('stg_app_db__users') }}
),

channel_mapping as (
    select * from {{ ref('utm_channel_mapping') }}
),

-- Extract distinct campaigns from user data
campaigns as (
    select distinct
        utm_source,
        utm_medium,
        utm_campaign
    from users
    where utm_campaign is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['c.utm_source', 'c.utm_medium', 'c.utm_campaign']) }} as campaign_id,
    c.utm_source,
    c.utm_medium,
    c.utm_campaign,
    coalesce(cm.channel, 'other') as channel
from campaigns c
left join channel_mapping cm
    on coalesce(c.utm_source, '') = coalesce(cm.utm_source, '')
    and coalesce(c.utm_medium, '') = coalesce(cm.utm_medium, '')
