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
    SUM(p_50_watched.`value`)                           AS video_p50_watched_actions,
    SUM(p_75_watched.`value`)                           AS video_p75_watched_actions,
    SUM(p_95_watched.`value`)                           AS video_p95_watched_actions,
    SUM(p_100_watched.`value`)                          AS video_p100_watched_actions,
    SUM(out_click.`value`)                              AS outbound_clicks
FROM
    {{ source('bigquery_nt_fb', 'gender_breakdown') }} ads
LEFT JOIN
    {{ source('bigquery_nt_fb', 'gender_breakdown_video_p_50_watched_actions') }} p_50_watched
    ON ads.ad_id = p_50_watched.ad_id AND ads.`date` = p_50_watched.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'gender_breakdown_video_p_75_watched_actions') }} p_75_watched
    ON ads.ad_id = p_75_watched.ad_id AND ads.`date` = p_75_watched.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'gender_breakdown_video_p_95_watched_actions') }} p_95_watched
    ON ads.ad_id = p_95_watched.ad_id AND ads.`date` = p_95_watched.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'gender_breakdown_video_p_100_watched_actions') }} p_100_watched
    ON ads.ad_id = p_100_watched.ad_id AND ads.`date` = p_100_watched.`date`
LEFT JOIN 
	{{ source('bigquery_nt_fb', 'gender_breakdown_outbound_clicks') }} out_click
    ON ads.ad_id = out_click.ad_id AND ads.`date` = out_click.`date`
GROUP BY
ads.`date`,
ads.ad_id,
ads.gender
ORDER BY ads.`date` DESC, ads.ad_id DESC