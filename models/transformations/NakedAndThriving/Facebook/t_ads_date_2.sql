{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    )
}}
-- with cte as(
SELECT
    ads.`date`,
    -- ads.campaign_id,
    -- ads.campaign_name,
    -- ads.adset_id,
    -- ads.adset_name,
    ads.ad_id,
    -- ads.ad_name,
    COALESCE (SUM(purchase_roas.`value`),0)             AS website_purchase_roas,
    SUM(out_click_ctr.`value`)                          AS outbound_clicks_ctr,
    SUM(cost_per_conv.`value`)                          AS cost_per_conversion,
    SUM(cost_per_out_conv.`value`)                      AS cost_per_outbound_click,
    SUM(cost_per_unique_out_click.`value`)              AS cost_per_unique_outbound_click,
    SUM(unique_out_click.`value`)                       AS unique_outbound_clicks,
    SUM(unique_out_click_ctr.`value`)                   AS unique_outbound_clicks_ctr,
    SUM(website_ctr.`value`)                            AS unique_website_ctr,
    SUM(thruplay.`value`)                               AS cost_per_thruplay,
    SUM(play_actions.`value`)                           AS video_views_3s,
    SUM(watched_actions.`value`)                        AS video_continuous_2_sec_watched,
    SUM(thruplay_watched_actions.`value`)               AS video_thruplay_watched,
    SUM(video_30s.`value`)                              AS video_views_30s,
    SUM(avg_time_watched.`value`)                       AS avg_watch_time,
    SUM(p_25_watched.`value`)                           AS video_p25_watched_actions,
    SUM(p_50_watched.`value`)                           AS video_p50_watched_actions,
    SUM(p_75_watched.`value`)                           AS video_p75_watched_actions,
    SUM(p_95_watched.`value`)                           AS video_p95_watched_actions,
    SUM(p_100_watched.`value`)                          AS video_p100_watched_actions,
    SUM(play_actions.`value`)                           AS video_play_actions,
    SUM(out_click.`value`)                              AS outbound_clicks
    --SUM(play_curve_actions.`value`)                     AS video_play_curve_actions,

    
    -- ROW_NUMBER() OVER (PARTITION BY ads.`date`, ads.ad_id) AS r_num
FROM
    {{ source('bigquery_nt_fb', 'ads_breakdown') }} ads
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_website_purchase_roas') }} purchase_roas
    ON ads.ad_id = purchase_roas.ad_id AND ads.`date` = purchase_roas.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_outbound_clicks_ctr') }} out_click_ctr
    ON ads.ad_id = out_click_ctr.ad_id AND ads.`date` = out_click_ctr.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_cost_per_conversion') }} cost_per_conv
    ON ads.ad_id = cost_per_conv.ad_id AND ads.`date` = cost_per_conv.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_cost_per_outbound_click') }} cost_per_out_conv
    ON ads.ad_id = cost_per_out_conv.ad_id AND ads.`date` = cost_per_out_conv.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_cost_per_unique_outbound_click') }} cost_per_unique_out_click
    ON ads.ad_id = cost_per_unique_out_click.ad_id AND ads.`date` = cost_per_unique_out_click.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_unique_outbound_clicks') }} unique_out_click
    ON ads.ad_id = unique_out_click.ad_id AND ads.`date` = unique_out_click.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_unique_outbound_clicks_ctr') }}unique_out_click_ctr
    ON ads.ad_id = unique_out_click_ctr.ad_id AND ads.`date` = unique_out_click_ctr.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_website_ctr') }} website_ctr
    ON ads.ad_id = website_ctr.ad_id AND ads.`date` = website_ctr.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_cost_per_thruplay') }} thruplay
    ON ads.ad_id = thruplay.ad_id AND ads.`date` = thruplay.`date`    
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_video_continuous_2_sec_watched_actions') }} watched_actions
    ON ads.ad_id = watched_actions.ad_id AND ads.`date` = watched_actions.`date`  
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_video_thruplay_watched_actions') }} thruplay_watched_actions
    ON ads.ad_id = thruplay_watched_actions.ad_id AND ads.`date` = thruplay_watched_actions.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_video_30_sec_watched_actions') }} video_30s
    ON ads.ad_id = video_30s.ad_id AND ads.`date` = video_30s.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_video_avg_time_watched_actions') }} avg_time_watched
    ON ads.ad_id = avg_time_watched.ad_id AND ads.`date` = avg_time_watched.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_video_p_25_watched_actions') }} p_25_watched
    ON ads.ad_id = p_25_watched.ad_id AND ads.`date` = p_25_watched.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_video_p_50_watched_actions') }} p_50_watched
    ON ads.ad_id = p_50_watched.ad_id AND ads.`date` = p_50_watched.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_video_p_75_watched_actions') }} p_75_watched
    ON ads.ad_id = p_75_watched.ad_id AND ads.`date` = p_75_watched.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_video_p_95_watched_actions') }} p_95_watched
    ON ads.ad_id = p_95_watched.ad_id AND ads.`date` = p_95_watched.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_video_p_100_watched_actions') }} p_100_watched
    ON ads.ad_id = p_100_watched.ad_id AND ads.`date` = p_100_watched.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_video_play_actions') }} play_actions
    ON ads.ad_id = play_actions.ad_id AND ads.`date` = play_actions.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ads_breakdown_video_play_curve_actions') }} play_curve_actions
    ON ads.ad_id = play_curve_actions.ad_id AND ads.`date` = play_curve_actions.`date`
LEFT JOIN {{ source('bigquery_nt_fb', 'ads_breakdown_outbound_clicks') }} out_click
    ON ads.ad_id = out_click.ad_id AND ads.`date` = out_click.`date`
GROUP BY
ads.`date`,
ads.ad_id
-- ads.adset_id,
-- ads.ad_name,
-- ads.adset_name,
-- ads.campaign_id,
-- ads.campaign_name
ORDER BY ads.`date` DESC, ads.ad_id DESC
-- )

-- select date, sum(video_p25_watched_actions) from cte group by date order by date desc