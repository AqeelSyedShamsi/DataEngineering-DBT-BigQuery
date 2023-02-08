{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    )
}}

SELECT
    ads.`date`,
    ads.ad_id,
    ads.country,
    SUM(thruplay.`value`)                               AS cost_per_thruplay,
    SUM(play_actions.`value`)                           AS video_views_3s
    
FROM
    {{ source('bigquery_nt_fb', 'country_breakdown') }} ads  
LEFT JOIN
    {{ source('bigquery_nt_fb', 'country_breakdown_cost_per_thruplay') }} thruplay
    ON ads.ad_id = thruplay.ad_id AND ads.`date` = thruplay.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'country_breakdown_video_play_actions') }} play_actions
    ON ads.ad_id = play_actions.ad_id AND ads.`date` = play_actions.`date`
GROUP BY
ads.`date`,
ads.ad_id,
ads.country
ORDER BY ads.`date` DESC, ads.ad_id DESC