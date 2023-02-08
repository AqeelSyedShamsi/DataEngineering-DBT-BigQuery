{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    ) 
}}

SELECT 
    ads.ad_id,
    ads.ad_name,
    img.permalink_url               AS ad_thumbnail,
FROM
    {{ source('bigquery_nt_fb', 'age_breakdown') }}  ads
JOIN
    {{ source('bigquery_nt_fb', 'ad_image_history') }}  img
    ON ads.ad_name LIKE CONCAT('%',img.`name`,'%')  
where
    img.permalink_url   IS NOT NULL