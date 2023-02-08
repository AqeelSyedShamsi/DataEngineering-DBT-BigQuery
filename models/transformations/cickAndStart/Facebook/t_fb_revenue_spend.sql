{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    )
}}

SELECT
    custom.`date`,
    SUM(custom.spend) AS spend,
    SUM(CASE WHEN action_value.action_type = 'offsite_conversion.fb_pixel_purchase' OR
              action_value.action_type = 'onsite_conversion.purchase' OR
              action_value.action_type = 'offline_conversion.purchase' OR
              action_value.action_type = 'omni_purchase' 
        THEN action_value.`value` 
        ELSE 0 
        END) AS `revenue`
FROM
    {{ source('bigquery_nt_fb', 'ads_breakdown') }} custom
JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_action_values') }} action_value 
ON custom.`date` = action_value.`date` AND custom.ad_id = action_value.ad_id
GROUP BY
    custom.`date`