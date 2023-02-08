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
        {{ source('bigquery_nt_fb', 'age_breakdown_cost_per_unique_action_type') }} 
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
) AS SourceTable PIVOT(SUM(`value`) FOR formated_action_type IN ("post_engagement",
"link_click",
"landing_page_view",
"page_engagement",
"onsite_conversion_post_save",
"video_view",
"omni_add_to_cart",
"omni_initiated_checkout",
"omni_view_content",
"omni_purchase",
"like",
"offsite_conversion_fb_pixel_custom",
"omni_complete_registration",
"omni_search"
) ) AS pivot_table

