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
        {{ source('bigquery_nt_fb', 'age_breakdown_actions') }}
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
"post",
"view_content",
"offsite_conversion_fb_pixel_view_content",
"omni_view_content",
"post_reaction",
"omni_add_to_cart",
"offsite_conversion_fb_pixel_add_to_cart",
"add_to_cart",
"onsite_conversion_post_save",
"video_view",
"omni_purchase",
"purchase",
"offsite_conversion_fb_pixel_purchase",
"initiate_checkout",
"offsite_conversion_fb_pixel_initiate_checkout",
"omni_initiated_checkout",
"comment",
"offsite_conversion_fb_pixel_custom",
"offsite_conversion_custom_3944373342300833",
"offsite_conversion_fb_pixel_complete_registration",
"omni_complete_registration",
"complete_registration",
"offsite_conversion_custom_217780862955451",
"offline_conversion_purchase",
"offsite_conversion_fb_pixel_search",
"omni_search",
"search",
"like",
"photo_view",
"offline_conversion_custom_428241847834223",
"offsite_conversion_custom_1079274109225348",
"offsite_conversion_custom_2727760374123718",
"offsite_conversion_custom_143882113714449",
"offsite_conversion_custom_1348239005553304",
"offsite_conversion_custom_688223331679048",
"offsite_conversion_custom_1247676242328240"
) ) AS pivot_table

