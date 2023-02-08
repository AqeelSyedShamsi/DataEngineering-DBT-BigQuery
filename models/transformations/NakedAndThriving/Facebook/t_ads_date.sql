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
    ads_2.cost_per_conversion,
    ads_2.cost_per_outbound_click,
    ads_2.cost_per_unique_outbound_click,
    ads_2.unique_outbound_clicks,
    ads_2.unique_outbound_clicks_ctr,
    ads_2.unique_website_ctr,
    ads_2.cost_per_thruplay,
    ads_2.video_views_3s,
    ads_2.video_continuous_2_sec_watched,
    ads_2.video_thruplay_watched,
    ads_2.video_views_30s,
    ads_2.avg_watch_time,
    ads_2.video_p25_watched_actions,
    ads_2.video_p50_watched_actions,
    ads_2.video_p75_watched_actions,
    ads_2.video_p95_watched_actions,
    ads_2.video_p100_watched_actions,
    ads_2.video_play_actions,
    ads_2.outbound_clicks,
    ROW_NUMBER() OVER (PARTITION BY ads_1.`date`, ads_1.ad_id) AS r_num
FROM
    {{ ref('t_ads_date_1') }} ads_1
JOIN
    {{ ref('t_ads_date_2') }} ads_2
    ON ads_1.`date` = ads_2.`date` 
    -- AND ads_1.campaign_id = ads_2.campaign_id
    -- AND ads_1.adset_id = ads_2.adset_id
    AND ads_1.ad_id = ads_2.ad_id 
)
select * from cte where r_num = 1