{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    )
}}

SELECT
a._fivetran_id,
    a.ad_id,
    a.`date`,
    ad_name,
    SPLIT(a.ad_name, '_')[safe_ordinal(1)] AS `ad_launch_date`,
    SPLIT(a.ad_name, '_')[safe_ordinal(2)] AS `client_code`,
    SPLIT(a.ad_name, '_')[safe_ordinal(4)] AS `creative_type`,
    SPLIT(a.ad_name, '_')[safe_ordinal(5)] AS `aspect_ratio`,
    SPLIT(a.ad_name, '_')[safe_ordinal(6)] AS `ad_concept`,
    SPLIT(a.ad_name, '_')[safe_ordinal(7)] AS `ad_angle`,
    SPLIT(a.ad_name, '_')[safe_ordinal(8)] AS `product_name`,
    SPLIT(a.ad_name, '_')[safe_ordinal(9)] AS `funnel`,
    SPLIT(a.ad_name, '_')[safe_ordinal(10)] AS `influencer_name`,
    SPLIT(a.ad_name, '_')[safe_ordinal(11)] AS `promotion`,
    SPLIT(a.ad_name, '_')[safe_ordinal(12)] AS `creative_manager`,
    SPLIT(a.ad_name, '_')[safe_ordinal(13)] AS `ad_designer`,
    SPLIT(a.ad_name, '_')[safe_ordinal(14)] AS `concept_stage`,
    SPLIT(a.ad_name, '_')[safe_ordinal(15)] AS `creative_brief_name`,
    REGEXP_REPLACE(SPLIT(a.ad_name, '_')[safe_ordinal(3)],'[^0-9 ].[^0-9 ]','') as `creatives_tested`,
    SPLIT(a.ad_name, '_')[safe_ordinal(3)] as `creative_requests_tested`,
    adset_id,
    adset_name,
    campaign_id,
    campaign_name,
    age,
    SUM(clicks) AS clicks,
    SUM(cpc) AS cpc,
    SUM(cpm) AS cpm,
    SUM(cpp) AS cpp,
    SUM(ctr) AS ctr,
    SUM(frequency) AS frequency,
    SUM(impressions) AS impressions,
    SUM(inline_link_clicks) AS inline_link_clicks,
    SUM(CAST(inline_post_engagement AS INTEGER)) AS inline_post_engagement,
    objective,
    optimization_goal,
    SUM(reach) AS reach,
    SUM(spend) AS spend,
    SUM(CASE WHEN c.action_type = 'offsite_conversion.fb_pixel_purchase' OR
              c.action_type = 'onsite_conversion.purchase' OR
              c.action_type = 'offline_conversion.purchase' OR
              c.action_type = 'omni_purchase' 
        THEN c.`value` 
        ELSE 0 
        END) AS `Revenue`,
    SUM(CASE WHEN b.action_type = 'offsite_conversion.fb_pixel_purchase' OR
                b.action_type = 'onsite_conversion.purchase' OR
                b.action_type = 'offline_conversion.purchase' OR
                b.action_type = 'omni_purchase' THEN b.`value` ELSE 0 END) AS `Purchases`
FROM
    {{ source('bigquery_nt_fb', 'age_breakdown') }} a
LEFT JOIN
    {{ source('bigquery_nt_fb', 'age_breakdown_actions') }} b
ON a._fivetran_id = b._fivetran_id AND a.ad_id = b.ad_id AND a.`date` = b.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'age_breakdown_action_values') }} c
ON a._fivetran_id = c._fivetran_id AND a.ad_id = c.ad_id AND a.`date` = c.`date`

--where a.`date` = '2022-07-27' and a.ad_id = '23850716690790260'
GROUP BY
a._fivetran_id,
a.`date`,
a.ad_id,
adset_id,
ad_name,
adset_name,
campaign_id,
campaign_name,
age,
objective,
optimization_goal
ORDER BY a.`date` DESC, ad_id DESC