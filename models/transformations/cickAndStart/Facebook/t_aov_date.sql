{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    )
}}

SELECT
    a.ad_id,
    a.`date`,
    a.ad_name,
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
    REPLACE(REGEXP_REPLACE(SPLIT(a.ad_name, '_')[safe_ordinal(3)],'[^0-9a-z ].',''), '.', '') as `creative_requests_tested`,
    SPLIT(a.ad_name, '_')[safe_ordinal(3)] as `creative_tested`,
    c.campaign_id,
    c.creative_id,
    -- d.thumbnail_url,
    -- REPLACE(REPLACE(f.post, '"]', ''), '["', 'https://www.facebook.com/') as link_to_post,
    SUM(CASE WHEN g.action_type = 'offsite_conversion.fb_pixel_purchase' OR
              g.action_type = 'onsite_conversion.purchase' OR
              g.action_type = 'offline_conversion.purchase' OR
              g.action_type = 'omni_purchase' 
        THEN g.`value` 
        ELSE 0 
        END) AS `revenue`,
    SUM(CASE WHEN b.action_type = 'offsite_conversion.fb_pixel_purchase' OR
                b.action_type = 'onsite_conversion.purchase' OR
                b.action_type = 'offline_conversion.purchase' OR
                b.action_type = 'omni_purchase' THEN b.`value` ELSE 0 END) AS `purchases`,
    COALESCE(h.`value`, 0) AS `outbound_clicks`,
    REPLACE(REPLACE(f.post, '"]', ''), '["', 'https://www.facebook.com/') as link_to_post
FROM
    {{ source('bigquery_nt_fb', 'ads_breakdown') }} a
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_actions') }} b
ON a.ad_id = b.ad_id AND a.`date` = b.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_action_values') }} g
ON a.ad_id = g.ad_id AND a.`date` = g.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ad_history') }} c
ON CAST(a.ad_id AS INTEGER) = c.id
-- LEFT JOIN
--     {{ source('bigquery_nt_fb', 'creative_history') }} d
-- ON c.creative_id = d.creative_id
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ad_conversion') }} f
ON c.id = f.ad_id
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_outbound_clicks') }} h
ON h.ad_id = a.ad_id AND h.`date` = a.`date`
GROUP BY ad_id,
        `date`,
        ad_name,
        REGEXP_REPLACE(SPLIT(a.ad_name, '_')[safe_ordinal(3)],'[^0-9 ].[^0-9 ]',''),
        SPLIT(a.ad_name, '_')[safe_ordinal(3)],
        c.creative_id,
        -- thumbnail_url,
        c.campaign_id,
        h.`value`,
        f.post
ORDER BY a.`date` DESC, a.ad_id DESC