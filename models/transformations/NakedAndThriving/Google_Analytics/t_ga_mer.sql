{{
    config(
        materialized='table',
        schema='naked_and_thriving_google_analytics'
    )
}}

WITH cte_total_revenue AS(
    SELECT
        `date`,
        SUM (revenue)  AS total_revenue
    FROM
        {{ ref('t_social_media_acquisitions') }}
    GROUP BY
        `date`
)
SELECT
    rev.`date`,
    rev.total_revenue / fb.total_spend    AS mer
FROM
    cte_total_revenue rev
JOIN
    {{ ref('t_fb_total_spend') }} fb
ON 
    rev.`date` = fb.`date`
ORDER BY
    rev.`date` DESC