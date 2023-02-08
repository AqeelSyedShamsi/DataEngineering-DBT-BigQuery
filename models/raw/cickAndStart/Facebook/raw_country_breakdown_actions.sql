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
        {{ source('bigquery_nt_fb', 'country_breakdown_actions') }}
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
"landing_page_view",
"link_click",
"post_engagement",
"offsite_conversion_fb_pixel_view_content",
"view_content",
"omni_view_content",
"post",
"onsite_conversion_post_save",
"post_reaction",
"omni_add_to_cart",
"offsite_conversion_fb_pixel_add_to_cart",
"add_to_cart",
"video_view",
"comment",
"omni_purchase",
"offsite_conversion_fb_pixel_purchase",
"purchase",
"offsite_conversion_fb_pixel_initiate_checkout",
"initiate_checkout",
"omni_initiated_checkout",
"like",
"offsite_conversion_fb_pixel_custom",
"offsite_conversion_custom_3944373342300833",
"offsite_conversion_custom_688223331679048",
"offsite_conversion_custom_1079274109225348",
"offsite_conversion_fb_pixel_complete_registration",
"omni_complete_registration",
"complete_registration",
"offline_conversion_purchase",
"offline_conversion_custom_428241847834223",
"offsite_conversion_custom_1247676242328240",
"offsite_conversion_custom_143882113714449",
"offsite_conversion_custom_2727760374123718",
"photo_view",
"offsite_conversion_fb_pixel_search",
"search",
"offsite_conversion_custom_1348239005553304",
"omni_search",
"offsite_conversion_custom_217780862955451"


) ) AS pivot_table

