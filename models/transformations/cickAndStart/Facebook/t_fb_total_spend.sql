{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    )
}}

SELECT
    `date`,
    SUM (spend) AS total_spend
FROM
    {{ source('bigquery_nt_fb', 'ads_breakdown') }}
GROUP BY
    `date`
