{{
    config(
        materialized='table',
        schema='naked_and_thriving_google_ads'
    )
}}

SELECT
    `date`,
    CAST(SPLIT(base_campaign, '/')[safe_ordinal(4)] AS INTEGER) as `campaign_id`,
    cost_micros/1000000 as `spend`,
    conversions_value as `revenue`
FROM
    {{ source('bigquery_nt_gads', 'campaign_stats') }}