WITH
insights AS (
    SELECT
        *
    FROM {{ ref('stg_facebook_insights_pipeline__facebook_insights') }}
),

insights__action_values AS (
    SELECT
        _dlt_parent_id,
        -- pivot the action types
        SUM(CASE WHEN action_type = 'offsite_conversion.fb_pixel_purchase' THEN value ELSE 0 END) AS offsite_conversion_fb_pixel_purchase,
        SUM(CASE WHEN action_type = 'offsite_conversion.fb_pixel_initiate_checkout' THEN value ELSE 0 END) AS offsite_conversion_fb_pixel_initiate_checkout,
        SUM(CASE WHEN action_type = 'offsite_conversion.fb_pixel_add_to_cart' THEN value ELSE 0 END) AS offsite_conversion_fb_pixel_add_to_cart,
        SUM(CASE WHEN action_type = 'offsite_conversion.fb_pixel_add_payment_info' THEN value ELSE 0 END) AS offsite_conversion_fb_pixel_add_payment_info
        -- action_type,
        -- value,
        -- _1d_view,
        -- _7d_click,
        -- _dlt_root_id,
        -- _dlt_parent_id,
        -- _dlt_list_idx,
        -- _dlt_id
    FROM {{ ref('stg_facebook_insights_pipeline__facebook_insights__action_values') }}
    WHERE action_type IN (
        'offsite_conversion.fb_pixel_purchase',
        'offsite_conversion.fb_pixel_initiate_checkout',
        'offsite_conversion.fb_pixel_add_to_cart',
        'offsite_conversion.fb_pixel_add_payment_info'
    )
    GROUP BY _dlt_parent_id
),

insights__actions AS (
    SELECT
        *
    FROM {{ ref('stg_facebook_insights_pipeline__facebook_insights__actions') }}
),

joined AS (
    SELECT
        insights.account_id,
        insights.campaign_id,
        insights.adset_id,
        insights.ad_id,
        insights.date_start,
        insights.reach,
        insights.impressions,
        insights.frequency,
        insights.clicks,
        insights.unique_clicks,
        insights.ctr,
        insights.unique_ctr,
        insights.cpc,
        insights.cpm,
        insights.cpp,
        insights.spend,
        insights._dlt_load_id,
        insights._dlt_id,
        -- action values
        insights__action_values._dlt_parent_id,
        insights__action_values.offsite_conversion_fb_pixel_purchase,
        insights__action_values.offsite_conversion_fb_pixel_initiate_checkout,
        insights__action_values.offsite_conversion_fb_pixel_add_to_cart,
        insights__action_values.offsite_conversion_fb_pixel_add_payment_info
        -- actions
        -- insights__actions.*
    FROM insights
    LEFT JOIN insights__action_values
        ON insights._dlt_id = insights__action_values._dlt_parent_id
    -- LEFT JOIN insights__actions
    --     ON insights._dlt_id = insights__actions._dlt_parent_id        
)

SELECT * FROM joined
-- -- just for debugging
-- WHERE CAST(date_start AS DATE) = '2024-11-23'
-- AND ad_id = '120212261211530204'
-- AND account_id = '1639738426109756'
-- AND campaign_id = '120205999721160204' -- 120205999721160210
