{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    )
}}

SELECT
    ads.`date`,
    ads.ad_id,
    SUM(out_click.`value`) AS outbound_clicks
FROM
    {{ source('bigquery_nt_fb', 'ads_breakdown') }}  ads
JOIN {{ source('bigquery_nt_fb', 'ads_breakdown_outbound_clicks') }} out_click
    ON ads.ad_id = out_click.ad_id AND ads.`date` = out_click.`date`
GROUP BY 
    ads.`date`,
    ads.ad_id
ORDER BY ads.`date` DESC