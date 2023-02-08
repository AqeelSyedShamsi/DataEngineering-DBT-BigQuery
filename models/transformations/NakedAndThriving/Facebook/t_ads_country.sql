{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    )
}}
SELECT
    ads_1.*,
    ads_2.video_p50_watched_actions,
    ads_2.video_p75_watched_actions,
    ads_2.unique_outbound_clicks,
    ads_2.unique_outbound_clicks_ctr,
    ads_2.cost_per_thruplay,
    ads_2.video_views_3s,
    ads_3.avg_watch_time,
    ads_3.video_p25_watched_actions,
    ads_3.video_p95_watched_actions,
    ads_3.video_p100_watched_actions,
    ads_3.outbound_clicks
FROM
    {{ ref('t_ads_country_main_1') }} ads_1
JOIN
    {{ ref('t_ads_country_main_2') }} ads_2
    ON ads_1.`date` = ads_2.`date` 
    AND ads_1.ad_id = ads_2.ad_id 
JOIN
    {{ ref('t_ads_country_main_3') }} ads_3
    ON ads_1.`date` = ads_3.`date` 
    AND ads_1.ad_id = ads_3.ad_id 