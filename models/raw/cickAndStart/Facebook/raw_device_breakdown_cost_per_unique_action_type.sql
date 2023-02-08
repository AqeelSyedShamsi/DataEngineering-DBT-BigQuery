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
        {{ source('bigquery_nt_fb', 'device_breakdown_cost_per_unique_action_type') }}
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
) AS SourceTable PIVOT(SUM(`value`) FOR formated_action_type IN ("landing_page_view",
"link_click",
"post_engagement",
"page_engagement",
"onsite_conversion_post_save",
"omni_view_content",
"omni_add_to_cart",
"omni_initiated_checkout",
"omni_purchase",
"video_view",
"like",
"omni_search",
"omni_complete_registration",
"offsite_conversion_fb_pixel_custom"
) ) AS pivot_table

