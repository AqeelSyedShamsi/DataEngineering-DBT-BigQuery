{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    )
}}

WITH cte AS(
    SELECT 
        `date`,
        ad_id,
        action_type,
        REPLACE(action_type, '.', '_') AS formated_action_type ,
        `value`
    FROM 
        {{ source('bigquery_nt_fb', 'age_breakdown_action_values') }}
)
SELECT 
    *
FROM
(
    SELECT 
        `date`,
        ad_id,
        formated_action_type,
        `value`
    FROM 
        cte
) AS SourceTable PIVOT(SUM(`value`) FOR formated_action_type IN ("add_to_cart",
"offsite_conversion_fb_pixel_add_to_cart",
"omni_add_to_cart",
"offsite_conversion_fb_pixel_purchase",
"omni_purchase",
"offline_conversion_purchase",
"offline_conversion_custom_428241847834223"
) ) AS pivot_table

