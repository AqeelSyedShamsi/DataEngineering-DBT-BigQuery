{{
    config(
        materialized='table',
        schema='naked_and_thriving_google_analytics'
    )
}}

SELECT
    *
FROM
    {{ source('naked_and_thriving_google_analytics', 'utm_data') }}
-- SELECT
--     count(*)
-- FROM
--     {{ source('naked_and_thriving_google_analytics', 'utm_data') }} a
-- LEFT JOIN
--     {{ source('naked_and_thriving_google_analytics', 'campaign_performance') }} b
-- ON a.campaign = b.campaign AND a.date = b.date
-- WHERE a.date = '2022-09-18'
