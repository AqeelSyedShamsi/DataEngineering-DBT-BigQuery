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
    SUM(out_click.`value`)                              AS outbound_clicks
FROM
    {{ source('bigquery_nt_fb', 'country_breakdown') }} ads
LEFT JOIN 
    {{ source('bigquery_nt_fb', 'country_breakdown_outbound_clicks') }} out_click
    ON ads.`date` = out_click.`date` AND ads.ad_id = out_click.ad_id
GROUP BY
ads.`date`,
ads.ad_id,
ads.country
ORDER BY ads.`date` DESC, ads.ad_id DESC