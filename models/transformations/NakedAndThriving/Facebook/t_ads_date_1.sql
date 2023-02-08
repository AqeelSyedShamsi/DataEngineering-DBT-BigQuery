{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    )
}}

WITH cte_ads AS (
    SELECT
        ads.`date`,
        ads.campaign_id,
        ads.campaign_name,
        ads.adset_id,
        ads.adset_name,
        ads.ad_id,
        ads.ad_name,
        SPLIT(ads.ad_name, '_')[safe_ordinal(1)] AS `ad_launch_date`,
        SPLIT(ads.ad_name, '_')[safe_ordinal(2)] AS `client_code`,
        SPLIT(ads.ad_name, '_')[safe_ordinal(4)] AS `creative_type`,
        SPLIT(ads.ad_name, '_')[safe_ordinal(5)] AS `aspect_ratio`,
        SPLIT(ads.ad_name, '_')[safe_ordinal(6)] AS `ad_concept`,
        SPLIT(ads.ad_name, '_')[safe_ordinal(7)] AS `ad_angle`,
        SPLIT(ads.ad_name, '_')[safe_ordinal(8)] AS `product_name`,
        SPLIT(ads.ad_name, '_')[safe_ordinal(9)] AS `funnel`,
        SPLIT(ads.ad_name, '_')[safe_ordinal(10)] AS `influencer_name`,
        SPLIT(ads.ad_name, '_')[safe_ordinal(11)] AS `promotion`,
        SPLIT(ads.ad_name, '_')[safe_ordinal(12)] AS `creative_manager`,
        SPLIT(ads.ad_name, '_')[safe_ordinal(13)] AS `ad_designer`,
        SPLIT(ads.ad_name, '_')[safe_ordinal(14)] AS `concept_stage`,
        SPLIT(ads.ad_name, '_')[safe_ordinal(15)] AS `creative_brief_name`,
        REPLACE(REGEXP_REPLACE(SPLIT(ads.ad_name, '_')[safe_ordinal(3)],'[^0-9a-z ].',''), '.', '') as `creative_requests_tested`,
        SPLIT(ads.ad_name, '_')[safe_ordinal(3)] as `creative_tested`,
        SUM(ads.clicks) AS clicks, 
        SUM(ads.cpc) AS cpc,
        SUM(ads.cpm) AS cpm,
        SUM(ads.cpp) AS cpp,
        SUM(ads.ctr) AS ctr,
        SUM(ads.frequency) AS frequency,
        SUM(ads.impressions) AS impressions,
        SUM(ads.inline_link_clicks) AS inline_link_clicks,
        SUM(CAST(ads.inline_post_engagement AS INTEGER)) AS inline_post_engagement,
        SUM(ads.reach) AS reach,
        SUM(ads.spend) AS cost
    FROM
        {{ source('bigquery_nt_fb', 'ads_breakdown') }}  ads
    GROUP BY
        ads.`date`,
        ads.ad_id,
        ads.adset_id,
        ads.ad_name,
        ads.adset_name,
        ads.campaign_id,
        ads.campaign_name
    ),
    cte AS (
    SELECT
        cte_ads.*,
        img.ad_thumbnail,
        video.link_to_post,
        actions.actions_video_view,
        actions.actions_page_engagement,
        actions.actions_post_engagement,
        actions.actions_landing_page_view,
        actions.actions_post,
        actions.actions_link_click,
        actions.actions_post_reaction,
        actions.actions_onsite_conversion_post_save,
        actions.actions_comment,
        actions.actions_omni_add_to_cart,
        actions.actions_add_to_cart,
        actions.actions_offsite_conversion_fb_pixel_add_to_cart,
        actions.actions_offsite_conversion_fb_pixel_view_content,
        actions.actions_omni_view_content,
        actions.actions_view_content,
        actions.actions_offsite_conversion_fb_pixel_initiate_checkout,
        actions.actions_omni_initiated_checkout,
        actions.actions_initiate_checkout,
        actions.actions_offsite_conversion_fb_pixel_custom,
        actions.actions_like,
        actions.actions_offsite_conversion_fb_pixel_purchase,
        actions.actions_omni_purchase,
        actions.actions_purchase,
        actions.actions_offsite_conversion_fb_pixel_complete_registration,
        actions.actions_omni_complete_registration,
        actions.actions_complete_registration,
        actions.actions_offsite_conversion_custom_217780862955451,
        actions.actions_offline_conversion_purchase,
        actions.actions_offsite_conversion_custom_3944373342300833,
        actions.actions_offsite_conversion_fb_pixel_search,
        actions.actions_omni_search,
        actions.actions_search,
        actions.actions_offsite_conversion_custom_2727760374123718,
        actions.actions_offsite_conversion_custom_1079274109225348,
        actions.actions_offsite_conversion_custom_143882113714449,
        actions.actions_offsite_conversion_custom_1348239005553304,
        actions.actions_offline_conversion_custom_428241847834223,
        actions.actions_photo_view,
        actions.actions_offsite_conversion_custom_688223331679048,
        actions.actions_offsite_conversion_custom_1247676242328240,
        actions.action_values_omni_add_to_cart,
        actions.action_values_add_to_cart,
        actions.action_values_offsite_conversion_fb_pixel_add_to_cart,
        actions.action_values_offsite_conversion_fb_pixel_purchase,
        actions.action_values_omni_purchase,
        actions.action_values_offline_conversion_purchase,
        actions.action_values_offline_conversion_custom_428241847834223,
        actions.cost_per_unique_action_link_click,
        actions.cost_per_unique_action_video_view,
        actions.cost_per_unique_action_onsite_conversion_post_save,
        actions.cost_per_unique_action_page_engagement,
        actions.cost_per_unique_action_post_engagement,
        actions.cost_per_unique_action_landing_page_view,
        actions.cost_per_unique_action_like,
        actions.cost_per_unique_action_omni_view_content,
        actions.cost_per_unique_action_omni_complete_registration,
        actions.cost_per_unique_action_omni_purchase,
        actions.cost_per_unique_action_omni_add_to_cart,
        actions.cost_per_unique_action_omni_initiated_checkout,
        actions.cost_per_unique_action_offsite_conversion_fb_pixel_custom,
        actions.cost_per_unique_action_omni_search,
        actions.cost_per_action_type_page_engagement,
        actions.cost_per_action_type_offsite_conversion_custom_3944373342300833,
        actions.cost_per_action_type_link_click,
        actions.cost_per_action_type_video_view,
        actions.cost_per_action_type_post_engagement,
        actions.cost_per_action_type_omni_view_content,
        actions.cost_per_action_type_view_content,
        actions.cost_per_action_type_omni_add_to_cart,
        actions.cost_per_action_type_omni_initiated_checkout,
        actions.cost_per_action_type_add_to_cart,
        actions.cost_per_action_type_initiate_checkout,
        actions.cost_per_action_type_omni_purchase,
        actions.cost_per_action_type_purchase,
        actions.cost_per_action_type_offsite_conversion_fb_pixel_custom,
        actions.cost_per_action_type_omni_complete_registration,
        actions.cost_per_action_type_offsite_conversion_custom_217780862955451,
        actions.cost_per_action_type_complete_registration,

        ROW_NUMBER() OVER (PARTITION BY cte_ads.`date`, cte_ads.ad_id) AS r_num

    FROM
        cte_ads
    LEFT JOIN
        {{ ref('t_ad_thumbnail_date') }} img
        ON cte_ads.ad_id = img.ad_id
    LEFT JOIN
        {{ ref('t_ad_link_date') }} video
        ON cte_ads.ad_id = video.ad_id
    LEFT JOIN
        {{ ref('t_ads_actions_date') }} actions 
        ON cte_ads.`date` = actions.`date` AND cte_ads.ad_id = actions.ad_id
)

SELECT 
    * Except(r_num) 
FROM
    cte 
WHERE 
    r_num = 1
ORDER BY `date` DESC