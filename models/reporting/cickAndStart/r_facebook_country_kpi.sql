{{
    config(
        materialized='table',
        schema='naked_and_thriving_reports'
    )
}}

SELECT
    *,
    CASE WHEN actions_purchase = 0 or actions_purchase IS NULL THEN 0 ELSE action_values_omni_purchase/actions_purchase END AS `average_order_value`,
    CASE WHEN actions_purchase = 0 or actions_purchase IS NULL THEN 0 ELSE outbound_clicks/actions_purchase END AS `conversion_rate`
FROM
    {{ ref('t_ads_country') }}
