{{
    config(
        materialized='table',
        schema='naked_and_thriving_google_analytics'
    )
}}

SELECT
    a.`date`,
    `profile`,
    social_network,
    transactions,
    transaction_revenue as `revenue`,
    sessions,
    new_users,
FROM
    {{ source('naked_and_thriving_google_analytics', 'social_media_acquisitions') }} a
ORDER BY a.`date` DESC