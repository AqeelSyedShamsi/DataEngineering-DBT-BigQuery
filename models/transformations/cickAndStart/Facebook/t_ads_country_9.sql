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
    SUM(p_95_watched.`value`)                           AS video_p95_watched_actions,
    SUM(p_100_watched.`value`)                          AS video_p100_watched_actions
    -- SUM(out_click.`value`)                              AS outbound_clicks
FROM
    {{ source('bigquery_nt_fb', 'country_breakdown') }} ads
LEFT JOIN
    {{ source('bigquery_nt_fb', 'country_breakdown_video_p_95_watched_actions') }} p_95_watched
    ON ads.ad_id = p_95_watched.ad_id AND ads.`date` = p_95_watched.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'country_breakdown_video_p_100_watched_actions') }} p_100_watched
    ON ads.ad_id = p_100_watched.ad_id AND ads.`date` = p_100_watched.`date`
GROUP BY
ads.`date`,
ads.ad_id,
ads.country
ORDER BY ads.`date` DESC, ads.ad_id DESC