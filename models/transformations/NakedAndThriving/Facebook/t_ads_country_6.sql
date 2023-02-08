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
    SUM(unique_out_click.`value`)                       AS unique_outbound_clicks,
    SUM(unique_out_click_ctr.`value`)                   AS unique_outbound_clicks_ctr
    
FROM
    {{ source('bigquery_nt_fb', 'country_breakdown') }} ads
LEFT JOIN
    {{ source('bigquery_nt_fb', 'country_breakdown_unique_outbound_clicks') }} unique_out_click
    ON ads.ad_id = unique_out_click.ad_id AND ads.`date` = unique_out_click.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'country_breakdown_unique_outbound_clicks_ctr') }}unique_out_click_ctr
    ON ads.ad_id = unique_out_click_ctr.ad_id AND ads.`date` = unique_out_click_ctr.`date`
GROUP BY
ads.`date`,
ads.ad_id,
ads.country
ORDER BY ads.`date` DESC, ads.ad_id DESC