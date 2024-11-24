with source as (
      select * from {{ source('facebook_insights_pipeline', 'facebook_insights') }}
),
renamed as (
    select
        {{ adapter.quote("campaign_id") }},
        {{ adapter.quote("adset_id") }},
        {{ adapter.quote("ad_id") }},
        {{ adapter.quote("date_start") }},
        {{ adapter.quote("date_stop") }},
        {{ adapter.quote("reach") }},
        {{ adapter.quote("impressions") }},
        {{ adapter.quote("frequency") }},
        {{ adapter.quote("clicks") }},
        {{ adapter.quote("unique_clicks") }},
        {{ adapter.quote("ctr") }},
        {{ adapter.quote("unique_ctr") }},
        {{ adapter.quote("cpc") }},
        {{ adapter.quote("cpm") }},
        {{ adapter.quote("cpp") }},
        {{ adapter.quote("spend") }},
        {{ adapter.quote("account_id") }},
        {{ adapter.quote("_dlt_load_id") }},
        {{ adapter.quote("_dlt_id") }}

    from source
)
select * from renamed
  