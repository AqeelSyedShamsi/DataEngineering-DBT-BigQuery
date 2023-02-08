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
        {{ source('bigquery_nt_fb', 'ads_breakdown_cost_per_unique_action_type') }} 
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
) AS SourceTable PIVOT(SUM(`value`) FOR formated_action_type IN ("link_click",
"video_view",
"onsite_conversion_post_save",
"page_engagement",
"post_engagement",
"landing_page_view",
"like",
"omni_view_content",
"omni_complete_registration",
"omni_purchase",
"omni_add_to_cart",
"omni_initiated_checkout",
"offsite_conversion_fb_pixel_custom",
"omni_search"

) ) AS pivot_table

