{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    )
}}

SELECT
    ads.`date`,
    ads.ad_id,
    ads.gender,
    SUM(cost_per_unique_out_click.`value`)              AS cost_per_unique_outbound_click,
    SUM(website_ctr.`value`)                            AS unique_website_ctr,
    SUM(thruplay.`value`)                               AS cost_per_thruplay,
    SUM(play_actions.`value`)                           AS video_views_3s,
    SUM(watched_actions.`value`)                        AS video_continuous_2_sec_watched
FROM
    {{ source('bigquery_nt_fb', 'gender_breakdown') }} ads
LEFT JOIN
    {{ source('bigquery_nt_fb', 'gender_breakdown_cost_per_unique_outbound_click') }} cost_per_unique_out_click
    ON ads.ad_id = cost_per_unique_out_click.ad_id AND ads.`date` = cost_per_unique_out_click.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'gender_breakdown_website_ctr') }} website_ctr
    ON ads.ad_id = website_ctr.ad_id AND ads.`date` = website_ctr.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'gender_breakdown_cost_per_thruplay') }} thruplay
    ON ads.ad_id = thruplay.ad_id AND ads.`date` = thruplay.`date`    
LEFT JOIN
    {{ source('bigquery_nt_fb', 'gender_breakdown_video_continuous_2_sec_watched_actions') }} watched_actions
    ON ads.ad_id = watched_actions.ad_id AND ads.`date` = watched_actions.`date`  
LEFT JOIN
    {{ source('bigquery_nt_fb', 'gender_breakdown_video_play_actions') }} play_actions
    ON ads.ad_id = play_actions.ad_id AND ads.`date` = play_actions.`date`
GROUP BY
ads.`date`,
ads.ad_id,
ads.gender
ORDER BY ads.`date` DESC, ads.ad_id DESC