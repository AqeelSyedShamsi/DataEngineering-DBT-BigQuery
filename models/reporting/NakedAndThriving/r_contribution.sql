{{
    config(
        materialized='table',
        schema='naked_and_thriving_reports'
    )
}}

SELECT
    `date`,
    spend,
    revenue,
    'Facebook' AS `source`
FROM
    {{ ref('t_fb_revenue_spend') }}

UNION ALL

SELECT
    `date`,
    spend,
    revenue,
    'Google Ads' AS `source`
FROM
    {{ ref('t_ad_stats') }}