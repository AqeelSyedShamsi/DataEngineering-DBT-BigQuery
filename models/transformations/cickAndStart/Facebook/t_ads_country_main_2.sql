{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    )
}}
with cte as (
SELECT
    ads_1.*,
    ads_5.video_p50_watched_actions,
    ads_5.video_p75_watched_actions,
    ads_6.unique_outbound_clicks,
    ads_6.unique_outbound_clicks_ctr,
    ads_7.cost_per_thruplay,
    ads_7.video_views_3s,
    ROW_NUMBER() OVER (PARTITION BY ads_1.`date`, ads_1.ad_id) AS r_num
FROM
    {{ ref('t_ads_country_1') }} ads_1
JOIN
    {{ ref('t_ads_country_5') }} ads_5
    ON ads_1.`date` = ads_5.`date` 
    AND ads_1.ad_id = ads_5.ad_id 
JOIN
    {{ ref('t_ads_country_6') }} ads_6
    ON ads_1.`date` = ads_6.`date` 
    AND ads_1.ad_id = ads_6.ad_id
JOIN
    {{ ref('t_ads_country_7') }} ads_7
    ON ads_1.`date` = ads_7.`date` 
    AND ads_1.ad_id = ads_7.ad_id 
)
select * from cte where r_num = 1