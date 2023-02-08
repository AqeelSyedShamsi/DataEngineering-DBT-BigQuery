{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    )
}}
with cte as (
SELECT
    ads_1.*,
    ads_2.website_purchase_roas,
    ads_2.outbound_clicks_ctr,
    ads_2.cost_per_outbound_click,
    ads_3.cost_per_unique_outbound_click,
    ads_3.unique_website_ctr,
    ads_3.video_continuous_2_sec_watched,
    ads_4.video_thruplay_watched,
    ads_4.video_views_30s,
    ROW_NUMBER() OVER (PARTITION BY ads_1.`date`, ads_1.ad_id) AS r_num
FROM
    {{ ref('t_ads_country_1') }} ads_1
JOIN
    {{ ref('t_ads_country_2') }} ads_2
    ON ads_1.`date` = ads_2.`date` 
    AND ads_1.ad_id = ads_2.ad_id 
JOIN
    {{ ref('t_ads_country_3') }} ads_3
    ON ads_1.`date` = ads_3.`date` 
    AND ads_1.ad_id = ads_3.ad_id 
JOIN
    {{ ref('t_ads_country_4') }} ads_4
    ON ads_1.`date` = ads_4.`date`
    AND ads_1.ad_id = ads_4.ad_id 
)
select * from cte where r_num = 1