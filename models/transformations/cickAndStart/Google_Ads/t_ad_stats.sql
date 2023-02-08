{{
    config(
        materialized='table',
        schema='naked_and_thriving_google_ads'
    )
}}

SELECT
    `date`,
    ad_id,
    campaign_id,
    SUM(cost_micros/1000000) AS `spend`,
    SUM(conversions_value) AS `revenue`
FROM
    {{ source('bigquery_nt_gads', 'ad_stats') }}
GROUP BY
    `date`,
    ad_id,
    campaign_id