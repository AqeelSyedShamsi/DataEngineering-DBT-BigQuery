{{
    config(
        materialized='table',
        schema='naked_and_thriving_reports'
    )
}}

SELECT
    COALESCE(p.`date`, m.`date`) AS `date`,
    m.mer,
    p.paid_cvr
FROM
    {{ ref('t_paid_cvr') }} p
FULL OUTER JOIN
    {{ ref('t_ga_mer') }} m
ON 
    p.`date` = m.`date`
ORDER BY
    COALESCE(p.`date`, m.`date`) DESC