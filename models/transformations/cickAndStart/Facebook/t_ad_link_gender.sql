{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    ) 
}} 

SELECT 
    ads.ad_id,
    ads.ad_name,
    video.`source`                  AS link_to_post
FROM
    {{ source('bigquery_nt_fb', 'gender_breakdown') }}  ads
LEFT JOIN
    {{ source('bigquery_nt_fb', 'ad_video_history') }} video
    ON ads.ad_name LIKE CONCAT('%', video.title, '%') 
where
    video.`source` IS NOT NULL