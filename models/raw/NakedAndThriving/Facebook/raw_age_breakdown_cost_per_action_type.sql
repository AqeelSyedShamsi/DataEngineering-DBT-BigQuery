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
        {{ source('bigquery_nt_fb', 'age_breakdown_cost_per_action_type') }}
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
) AS SourceTable PIVOT(SUM(`value`) FOR formated_action_type IN ("page_engagement",
"post_engagement",
"landing_page_view",
"link_click",
"onsite_conversion_post_save",
"video_view",
"view_content",
"initiate_checkout",
"omni_view_content",
"omni_add_to_cart",
"omni_initiated_checkout",
"add_to_cart",
"omni_purchase",
"purchase",
"like",
"offsite_conversion_fb_pixel_custom",
"omni_complete_registration",
"offsite_conversion_custom_217780862955451",
"complete_registration",
"offsite_conversion_custom_3944373342300833",
"omni_search",
"search",
"offline_conversion_custom_428241847834223",
"offsite_conversion_custom_1247676242328240",
"offsite_conversion_custom_1079274109225348",
"offsite_conversion_custom_2727760374123718",
"offsite_conversion_custom_1348239005553304",
"offsite_conversion_custom_143882113714449",
"offsite_conversion_custom_688223331679048"
) ) AS pivot_table

