{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    )
}}
with cte as (
SELECT
    ads_1.*,
    ads_8.avg_watch_time,
    ads_8.video_p25_watched_actions,
    ads_9.video_p95_watched_actions,
    ads_9.video_p100_watched_actions,
    ads_10.outbound_clicks,
    ROW_NUMBER() OVER (PARTITION BY ads_1.`date`, ads_1.ad_id) AS r_num
FROM
    {{ ref('t_ads_country_1') }} ads_1
JOIN
    {{ ref('t_ads_country_8') }} ads_8
    ON ads_1.`date` = ads_8.`date` 
    AND ads_1.ad_id = ads_8.ad_id 
JOIN
    {{ ref('t_ads_country_9') }} ads_9
    ON ads_1.`date` = ads_9.`date` 
    AND ads_1.ad_id = ads_9.ad_id 
JOIN
    {{ ref('t_ads_country_10') }} ads_10
    ON ads_1.`date` = ads_10.`date` 
    AND ads_1.ad_id = ads_10.ad_id 
)
select * from cte where r_num = 1