{{
    config(
        materialized='table',
        schema='naked_and_thriving_google_analytics'
    )
}}

SELECT
    `date`,
    --`medium`,
    SUM(transactions) / SUM(sessions) AS paid_cvr
FROM
    {{ source('bigquery_nt_ga', 'performance_breakdown') }}
WHERE
    `medium` = 'paid_social'
GROUP BY
    `date`
ORDER BY
    `date` DESC