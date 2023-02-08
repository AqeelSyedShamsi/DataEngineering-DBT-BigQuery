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
    COALESCE (SUM(purchase_roas.`value`),0)             AS website_purchase_roas,
    SUM(out_click_ctr.`value`)                          AS outbound_clicks_ctr,
    SUM(cost_per_out_conv.`value`)                      AS cost_per_outbound_click,
    SUM(unique_out_click.`value`)                       AS unique_outbound_clicks,
    SUM(unique_out_click_ctr.`value`)                   AS unique_outbound_clicks_ctr
FROM
    {{ source('bigquery_nt_fb', 'age_breakdown') }} ads
LEFT JOIN
    {{ source('bigquery_nt_fb', 'age_breakdown_website_purchase_roas') }} purchase_roas
    ON ads.ad_id = purchase_roas.ad_id AND ads.`date` = purchase_roas.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'age_breakdown_outbound_clicks_ctr') }} out_click_ctr
    ON ads.ad_id = out_click_ctr.ad_id AND ads.`date` = out_click_ctr.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'age_breakdown_cost_per_outbound_click') }} cost_per_out_conv
    ON ads.ad_id = cost_per_out_conv.ad_id AND ads.`date` = cost_per_out_conv.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'age_breakdown_unique_outbound_clicks') }} unique_out_click
    ON ads.ad_id = unique_out_click.ad_id AND ads.`date` = unique_out_click.`date`
LEFT JOIN
    {{ source('bigquery_nt_fb', 'age_breakdown_unique_outbound_clicks_ctr') }}unique_out_click_ctr
    ON ads.ad_id = unique_out_click_ctr.ad_id AND ads.`date` = unique_out_click_ctr.`date`
GROUP BY
ads.`date`,
ads.ad_id,
ads.age
ORDER BY ads.`date` DESC, ads.ad_id DESC