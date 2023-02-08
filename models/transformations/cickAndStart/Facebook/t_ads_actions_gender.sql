{{
    config(
        materialized='table',
        schema='naked_and_thriving_fb_ads'
    )
}}

SELECT
    ads.`date`,
    ads.ad_id,
    SUM(actions.video_view) AS actions_video_view,
    SUM(actions.page_engagement) AS actions_page_engagement,
    SUM(actions.post_engagement) AS actions_post_engagement,
    SUM(actions.landing_page_view) AS actions_landing_page_view,
    SUM(actions.post) AS actions_post,
    SUM(actions.link_click) AS actions_link_click,
    SUM(actions.post_reaction) AS actions_post_reaction,
    SUM(actions.onsite_conversion_post_save) AS actions_onsite_conversion_post_save,
    SUM(actions.comment) AS actions_comment,
    SUM(actions.omni_add_to_cart) AS actions_omni_add_to_cart,
    SUM(actions.add_to_cart) AS actions_add_to_cart,
    SUM(actions.offsite_conversion_fb_pixel_add_to_cart) AS actions_offsite_conversion_fb_pixel_add_to_cart,
    SUM(actions.offsite_conversion_fb_pixel_view_content) AS actions_offsite_conversion_fb_pixel_view_content,
    SUM(actions.omni_view_content) AS actions_omni_view_content,
    SUM(actions.view_content) AS actions_view_content,
    SUM(actions.offsite_conversion_fb_pixel_initiate_checkout) AS actions_offsite_conversion_fb_pixel_initiate_checkout,
    SUM(actions.omni_initiated_checkout) AS actions_omni_initiated_checkout,
    SUM(actions.initiate_checkout) AS actions_initiate_checkout,
    SUM(actions.offsite_conversion_fb_pixel_custom) AS actions_offsite_conversion_fb_pixel_custom,
    SUM(actions.`like`) AS actions_like,
    SUM(actions.offsite_conversion_fb_pixel_purchase) AS actions_offsite_conversion_fb_pixel_purchase,
    SUM(actions.omni_purchase) AS actions_omni_purchase,
    SUM(actions.purchase) AS actions_purchase,
    SUM(actions.offsite_conversion_fb_pixel_complete_registration) AS actions_offsite_conversion_fb_pixel_complete_registration,
    SUM(actions.omni_complete_registration) AS actions_omni_complete_registration,
    SUM(actions.complete_registration) AS actions_complete_registration,
    SUM(actions.offsite_conversion_custom_217780862955451) AS actions_offsite_conversion_custom_217780862955451,
    SUM(actions.offline_conversion_purchase) AS actions_offline_conversion_purchase,
    SUM(actions.offsite_conversion_custom_3944373342300833) AS actions_offsite_conversion_custom_3944373342300833,
    SUM(actions.offsite_conversion_fb_pixel_search) AS actions_offsite_conversion_fb_pixel_search,
    SUM(actions.omni_search) AS actions_omni_search,
    SUM(actions.`search`) AS actions_search,
    SUM(actions.offsite_conversion_custom_2727760374123718) AS actions_offsite_conversion_custom_2727760374123718,
    SUM(actions.offsite_conversion_custom_1079274109225348) AS actions_offsite_conversion_custom_1079274109225348,
    SUM(actions.offsite_conversion_custom_143882113714449) AS actions_offsite_conversion_custom_143882113714449,
    SUM(actions.offsite_conversion_custom_1348239005553304) AS actions_offsite_conversion_custom_1348239005553304,
    SUM(actions.offline_conversion_custom_428241847834223) AS actions_offline_conversion_custom_428241847834223,
    SUM(actions.photo_view) AS actions_photo_view,
    SUM(actions.offsite_conversion_custom_688223331679048) AS actions_offsite_conversion_custom_688223331679048,
    SUM(actions.offsite_conversion_custom_1247676242328240) AS actions_offsite_conversion_custom_1247676242328240,


    
    SUM(actions_values.omni_add_to_cart) AS action_values_omni_add_to_cart,
    SUM(actions_values.add_to_cart) AS action_values_add_to_cart,
    SUM(actions_values.offsite_conversion_fb_pixel_add_to_cart) AS action_values_offsite_conversion_fb_pixel_add_to_cart,
    SUM(actions_values.offsite_conversion_fb_pixel_purchase) AS action_values_offsite_conversion_fb_pixel_purchase,
    SUM(actions_values.omni_purchase) AS action_values_omni_purchase,
    SUM(actions_values.offline_conversion_purchase) AS action_values_offline_conversion_purchase,
    SUM(actions_values.offline_conversion_custom_428241847834223) AS action_values_offline_conversion_custom_428241847834223,


    SUM(cost_per_unique_action.link_click) AS cost_per_unique_action_link_click,
    SUM(cost_per_unique_action.video_view) AS cost_per_unique_action_video_view,
    SUM(cost_per_unique_action.onsite_conversion_post_save) AS cost_per_unique_action_onsite_conversion_post_save,
    SUM(cost_per_unique_action.page_engagement) AS cost_per_unique_action_page_engagement,
    SUM(cost_per_unique_action.post_engagement) AS cost_per_unique_action_post_engagement,
    SUM(cost_per_unique_action.landing_page_view) AS cost_per_unique_action_landing_page_view,
    SUM(cost_per_unique_action.`like`) AS cost_per_unique_action_like,
    SUM(cost_per_unique_action.omni_view_content) AS cost_per_unique_action_omni_view_content,
    SUM(cost_per_unique_action.omni_complete_registration) AS cost_per_unique_action_omni_complete_registration,
    SUM(cost_per_unique_action.omni_purchase) AS cost_per_unique_action_omni_purchase,
    SUM(cost_per_unique_action.omni_add_to_cart) AS cost_per_unique_action_omni_add_to_cart,
    SUM(cost_per_unique_action.omni_initiated_checkout) AS cost_per_unique_action_omni_initiated_checkout,
    SUM(cost_per_unique_action.offsite_conversion_fb_pixel_custom) AS cost_per_unique_action_offsite_conversion_fb_pixel_custom,
    SUM(cost_per_unique_action.omni_search) AS cost_per_unique_action_omni_search,

    SUM(cost_per_action_type.page_engagement) AS cost_per_action_type_page_engagement,
    SUM(cost_per_action_type.offsite_conversion_custom_3944373342300833) AS cost_per_action_type_offsite_conversion_custom_3944373342300833,
    SUM(cost_per_action_type.link_click) AS cost_per_action_type_link_click,
    SUM(cost_per_action_type.video_view) AS cost_per_action_type_video_view,
    SUM(cost_per_action_type.post_engagement) AS cost_per_action_type_post_engagement,
    SUM(cost_per_action_type.omni_view_content) AS cost_per_action_type_omni_view_content,
    SUM(cost_per_action_type.view_content) AS cost_per_action_type_view_content,
    SUM(cost_per_action_type.omni_add_to_cart) AS cost_per_action_type_omni_add_to_cart,
    SUM(cost_per_action_type.omni_initiated_checkout) AS cost_per_action_type_omni_initiated_checkout,
    SUM(cost_per_action_type.add_to_cart) AS cost_per_action_type_add_to_cart,
    SUM(cost_per_action_type.initiate_checkout) AS cost_per_action_type_initiate_checkout,
    SUM(cost_per_action_type.omni_purchase) AS cost_per_action_type_omni_purchase,
    SUM(cost_per_action_type.purchase) AS cost_per_action_type_purchase,
    SUM(cost_per_action_type.offsite_conversion_fb_pixel_custom) AS cost_per_action_type_offsite_conversion_fb_pixel_custom,
    SUM(cost_per_action_type.omni_complete_registration) AS cost_per_action_type_omni_complete_registration,
    SUM(cost_per_action_type.offsite_conversion_custom_217780862955451) AS cost_per_action_type_offsite_conversion_custom_217780862955451,
    SUM(cost_per_action_type.complete_registration) AS cost_per_action_type_complete_registration

FROM
    {{ source('bigquery_nt_fb', 'gender_breakdown') }}  ads
JOIN
    {{ ref('raw_gender_breakdown_actions') }}  actions
    ON ads.ad_id = actions.ad_id AND ads.`date` = actions.`date`
JOIN
    {{ ref('raw_gender_breakdown_action_values') }} actions_values
    ON ads.ad_id = actions_values.ad_id AND ads.`date` = actions_values.`date`
JOIN
    {{ ref('raw_gender_breakdown_cost_per_unique_action_type') }} cost_per_unique_action
    ON ads.ad_id = cost_per_unique_action.ad_id AND ads.`date` = cost_per_unique_action.`date`
JOIN
    {{ ref('raw_gender_breakdown_cost_per_action_type') }} cost_per_action_type
    ON ads.ad_id = cost_per_action_type.ad_id AND ads.`date` = cost_per_action_type.`date`

GROUP BY 
    ads.`date`,
    ads.ad_id
