with source as (
      select * from {{ source('facebook_insights_pipeline', 'facebook_insights__actions') }}
),
renamed as (
    select
        {{ adapter.quote("action_type") }},
        CAST({{ adapter.quote("value") }} AS DECIMAL) AS value,
        {{ adapter.quote("_1d_view") }},
        {{ adapter.quote("_7d_click") }},
        {{ adapter.quote("_dlt_root_id") }},
        {{ adapter.quote("_dlt_parent_id") }},
        {{ adapter.quote("_dlt_list_idx") }},
        {{ adapter.quote("_dlt_id") }}

    from source
)
select * from renamed
  