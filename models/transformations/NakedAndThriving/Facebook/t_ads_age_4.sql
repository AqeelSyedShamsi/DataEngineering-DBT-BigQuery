{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    )
}}

SELECT
    ads.`date`,
    ads.ad_id,
    ads.age,
    SUM(thruplay_watched_actions.`value`)               AS video_thruplay_watched,
    SUM(video_30s.`value`)                              AS video_views_30s,
    SUM(avg_time_watched.`value`)                       AS avg_watch_time,
    SUM(p_25_watched.`value`)                           AS video_p25_watched_actions
FROM
    {{ source('bigquery_nt_fb', 'age_breakdown') }} ads
LEFT JOIN
    {{ source('bigquery_nt_fb', 'age_breakdown_video_thruplay_watched_actions') }} thruplay_watched_actions
    ON ads.ad_id = thruplay_watched_actions.ad_id AND ads.`date` = thruplay_watched_actions.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'age_breakdown_video_30_sec_watched_actions') }} video_30s
    ON ads.ad_id = video_30s.ad_id AND ads.`date` = video_30s.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'age_breakdown_video_avg_time_watched_actions') }} avg_time_watched
    ON ads.ad_id = avg_time_watched.ad_id AND ads.`date` = avg_time_watched.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'age_breakdown_video_p_25_watched_actions') }} p_25_watched
    ON ads.ad_id = p_25_watched.ad_id AND ads.`date` = p_25_watched.`date`
GROUP BY
ads.`date`,
ads.ad_id,
ads.age
ORDER BY ads.`date` DESC, ads.ad_id DESC