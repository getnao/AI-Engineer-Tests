{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('mart_arr_all','mart_arr_with_zero_dollar_charges'),
    ('dim_subscription', 'dim_subscription'),
    ('mart_ping_instance', 'mart_ping_instance'),
    ('dim_product_detail', 'dim_product_detail'),
    ('map_installation_subscription_product', 'map_installation_subscription_product'),
    ('rpt_product_usage_health_score', 'rpt_product_usage_health_score'),
    ('dim_crm_account', 'dim_crm_account'),
    ('mart_behavior_structured_event_ai_gateway_flattened', 'mart_behavior_structured_event_ai_gateway_flattened')

    ])
}},

-- Identify subscriptions with Duo Core enabled
duo_core_enabled_subscriptions AS (

    SELECT
      snapshot_month,
      dim_subscription_id_original,
      delivery_type,
      dim_namespace_id,
      dim_installation_id,
      is_duo_core_features_enabled::BOOLEAN AS is_duo_core_features_enabled,
      duo_core_license_user_count,
      duo_core_billable_user_count
    FROM rpt_product_usage_health_score
    WHERE is_duo_core_features_enabled = TRUE
      AND snapshot_month >= '2025-04-01'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            snapshot_month, 
            dim_subscription_id_original, 
            delivery_type 
        ORDER BY
            billable_user_count DESC NULLS LAST, 
            ping_created_at DESC NULLS LAST
        ) = 1

),

monthly_duo_seats AS (

    SELECT
        mart_arr_all.arr_month                                         AS reporting_month,
        mart_arr_all.subscription_name,
        mart_arr_all.dim_subscription_id,
        mart_arr_all.dim_subscription_id_original,
        mart_arr_all.crm_account_name,
        mart_arr_all.dim_crm_account_id,
        mart_arr_all.dim_parent_crm_account_id,
        mart_arr_all.product_deployment_type                           AS product_deployment,
        CASE
            WHEN LOWER(mart_arr_all.product_rate_plan_charge_name) LIKE '%duo%' 
                THEN SPLIT_PART(mart_arr_all.product_rate_plan_category, ' - ', 2)
            -- For base-tier records where Duo Core is enabled, this categorizes the record as Duo Core
            WHEN duo_core_enabled_subscriptions.is_duo_core_features_enabled = TRUE 
                THEN 'GitLab Duo Core'
        END AS add_on_name,
        -- For base-tier records where Duo Core is enabled, there isn't any actual Duo subscription so set the quantity to be 0 and is_duo_subscription_paid to FALSE
        SUM(CASE WHEN add_on_name != 'GitLab Duo Core'
              THEN mart_arr_all.quantity
              ELSE 0 END)                                              AS d_seats,   
        SUM(mart_arr_all.arr)                                          AS duo_arr,
        IFF((SUM(mart_arr_all.arr) > 0) 
            AND (add_on_name != 'GitLab Duo Core'), TRUE, FALSE)       AS is_duo_subscription_paid,
        mart_arr_all.turn_on_cloud_licensing,
        CASE 
            WHEN mart_arr_all.turn_on_cloud_licensing = 'Offline' THEN 'Offline Cloud License'
            WHEN mart_arr_all.turn_on_cloud_licensing = 'No' THEN 'Legacy License File'
            WHEN mart_arr_all.turn_on_cloud_licensing = 'Yes' THEN 'Standard Cloud License'
            WHEN mart_arr_all.turn_on_cloud_licensing = '' THEN 'Standard Cloud License'
            ELSE 'error'
        END                                                            AS license_type,
        dim_crm_account.account_owner,
        dim_crm_account.parent_crm_account_geo,
        dim_crm_account.parent_crm_account_sales_segment,
        dim_crm_account.parent_crm_account_industry,
        dim_crm_account.technical_account_manager,
        duo_core_enabled_subscriptions.is_duo_core_features_enabled,
        duo_core_enabled_subscriptions.duo_core_license_user_count,
        duo_core_enabled_subscriptions.duo_core_billable_user_count
    FROM mart_arr_all
    LEFT JOIN dim_crm_account
        ON mart_arr_all.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN duo_core_enabled_subscriptions
        ON mart_arr_all.dim_subscription_id_original = duo_core_enabled_subscriptions.dim_subscription_id_original
        AND mart_arr_all.arr_month = duo_core_enabled_subscriptions.snapshot_month
        AND mart_arr_all.product_delivery_type = duo_core_enabled_subscriptions.delivery_type
    WHERE mart_arr_all.arr_month BETWEEN '2024-02-01' AND CURRENT_DATE -- first duo pro arr
        AND (
          LOWER(mart_arr_all.product_rate_plan_charge_name) LIKE '%duo%'  -- Paid Duo add-ons
      OR (
          duo_core_enabled_subscriptions.is_duo_core_features_enabled = TRUE  -- Only include if Duo Core is enabled
          AND mart_arr_all.is_licensed_user_base_product = TRUE 
          AND (LOWER(mart_arr_all.product_tier_name) LIKE '%premium%' OR LOWER(mart_arr_all.product_tier_name) LIKE '%ultimate%')
      )
    )
    GROUP BY ALL

),

duo_with_tiers AS ( --tier occurring concurrently with paid duo pro subscription

    SELECT
        monthly_duo_seats.*,
        dim_product_detail.is_oss_or_edu_rate_plan,
        -- multiple product tiers can show up within the same ARR reporting month
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT SPLIT_PART(mart_arr_all.product_tier_name, ' - ', 2)), ', ')    AS paired_tier_name,
        -- not able to sort within group while using SPLIT_PART function - using this method for standard results
        IFF(paired_tier_name IN ('Premium, Ultimate', 'Ultimate, Premium'), 'Premium & Ultimate', 
            paired_tier_name)                                                                              AS clean_paired_tier
    FROM monthly_duo_seats
    LEFT JOIN mart_arr_all -- joining to get tier occurring within same month as add on
        ON monthly_duo_seats.reporting_month = mart_arr_all.arr_month
        AND monthly_duo_seats.dim_crm_account_id = mart_arr_all.dim_crm_account_id
        AND monthly_duo_seats.dim_subscription_id = mart_arr_all.dim_subscription_id -- add on will be on the same subscription as the tier
        AND mart_arr_all.is_licensed_user_base_product = TRUE
    LEFT JOIN dim_product_detail
        ON mart_arr_all.dim_product_detail_id = dim_product_detail.dim_product_detail_id
    GROUP BY ALL

),

sm_dedicated_duo_info AS ( --CTE purpose is to get product info about sm and dedicated instances even if they don't trigger AI Gateway events

    SELECT
        duo_with_tiers.*,
        map_installation_subscription_product.dim_installation_id                               AS product_entity_id,
        IFF(map_installation_subscription_product.dim_installation_id IS NOT NULL, TRUE, FALSE) AS is_product_entity_associated_w_subscription,
        MAX(mart_ping_instance.major_minor_version_id)                                          AS major_minor_version_id, --max major minor version within month
        MAX(duo_with_tiers.d_seats)                                                             AS duo_seats -- max because left join can result in duplicate records
    FROM duo_with_tiers
    LEFT JOIN map_installation_subscription_product
        ON duo_with_tiers.dim_subscription_id_original = map_installation_subscription_product.dim_subscription_id_original
        AND DATE_TRUNC(MONTH, map_installation_subscription_product.date_actual) = duo_with_tiers.reporting_month
    LEFT JOIN mart_ping_instance
        ON map_installation_subscription_product.dim_installation_id = mart_ping_instance.dim_installation_id
        AND mart_ping_instance.ping_created_date_month = DATE_TRUNC(MONTH, map_installation_subscription_product.date_actual)
        AND mart_ping_instance.is_last_ping_of_month = TRUE
    WHERE duo_with_tiers.product_deployment IN ('Self-Managed', 'Dedicated')
    GROUP BY ALL

),

dotcom_duo_info AS ( --CTE purpose is to get product info about .com namespaces even if they don't trigger AI Gateway events

    SELECT
        duo_with_tiers.*,
        dim_subscription.namespace_id                               AS product_entity_id,
        IFF(dim_subscription.namespace_id IS NOT NULL, TRUE, FALSE) AS is_product_entity_associated_w_subscription,
        MAX(mart_ping_instance.major_minor_version_id)              AS major_minor_version_id, --max major minor version within month
        MAX(duo_with_tiers.d_seats)                                 AS duo_seats -- max because left join can result in duplicate records
    FROM duo_with_tiers
    LEFT JOIN dim_subscription
        ON dim_subscription.dim_subscription_id = duo_with_tiers.dim_subscription_id
    LEFT JOIN mart_ping_instance
        ON duo_with_tiers.reporting_month = mart_ping_instance.ping_created_date_month
        AND mart_ping_instance.dim_installation_id = '8b52effca410f0a380b0fcffaa1260e7' -- installation id for Gitlab.com
        AND mart_ping_instance.is_last_ping_of_month = TRUE
    WHERE duo_with_tiers.product_deployment = 'GitLab.com'
    GROUP BY ALL

),

duo_seat_assignments AS (
    /* 
    in this CTE, we are mirroring the logic from the Health Scoring lineage to get Duo license utilization
    at the subscription level. As discussed in this thread, the License users in this model already tie
    out, so we are only grabbing billable users.
    https://gitlab.com/gitlab-data/analytics/-/merge_requests/11765#note_2384070763
    */

    SELECT
        snapshot_month                                                                          AS reporting_month,
        COALESCE(dim_installation_id::VARCHAR, dim_namespace_id::VARCHAR)                       AS product_entity_id,
        deployment_type,
        CASE
          WHEN duo_pro_billable_user_count IS NOT NULL THEN 'GitLab Duo Pro'
          WHEN duo_enterprise_billable_user_count IS NOT NULL THEN 'GitLab Duo Enterprise'
          WHEN duo_amazon_q_billable_user_count IS NOT NULL THEN 'GitLab Duo with Amazon Q'
        END                                                                                     AS add_on_type,
        COALESCE(duo_pro_billable_user_count,
            duo_enterprise_billable_user_count, duo_amazon_q_billable_user_count)               AS duo_billable_user_count
    FROM rpt_product_usage_health_score
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            snapshot_month, 
            dim_subscription_id_original, 
            delivery_type 
        ORDER BY
            billable_user_count DESC NULLS LAST, 
            ping_created_at DESC NULLS LAST
        ) = 1
),

combined_duo_info AS (

    SELECT * FROM sm_dedicated_duo_info

    UNION ALL

    SELECT * FROM dotcom_duo_info

),

final AS (

    SELECT
        combined_duo_info.reporting_month,
        combined_duo_info.subscription_name,
        combined_duo_info.dim_subscription_id,
        combined_duo_info.crm_account_name,
        combined_duo_info.dim_crm_account_id,
        combined_duo_info.dim_parent_crm_account_id,
        combined_duo_info.product_deployment,
        combined_duo_info.add_on_name,
        combined_duo_info.clean_paired_tier                                                                      AS paired_tier,
        combined_duo_info.is_product_entity_associated_w_subscription,
        combined_duo_info.is_duo_subscription_paid,
        combined_duo_info.is_duo_core_features_enabled,
        MAX(combined_duo_info.major_minor_version_id)                                                            AS major_minor_version_id,
        CASE
            WHEN combined_duo_info.add_on_name = 'GitLab Duo Core'
                -- Consider Duo Core License Users instead of Base Tier License User, and Duo Seats Count for the Other Duo Add-ons. 
                THEN ZEROIFNULL(MAX(combined_duo_info.duo_core_license_user_count))
            ELSE ZEROIFNULL(MAX(combined_duo_info.duo_seats))
        END                                                                                                      AS paid_duo_seats,
        CASE
            WHEN combined_duo_info.add_on_name = 'GitLab Duo Core'
                -- Consider Duo Core Billable Users for Duo Core, and Seat Assigned Count for the Other Duo Add-ons.
                THEN ZEROIFNULL(MAX(combined_duo_info.duo_core_billable_user_count))
            ELSE ZEROIFNULL(MAX(duo_seat_assignments.duo_billable_user_count))
        END                                                                                                      AS count_seats_assigned,
        CASE
            WHEN combined_duo_info.add_on_name = 'GitLab Duo Core' 
            -- For Duo Core records: Only count users where enabled_by_duo_add_on = 'duo_core'
            -- For other add-on records: Count users but exclude those with enabled_by_duo_add_on != 'duo_core'
                THEN ZEROIFNULL(COUNT(DISTINCT CASE WHEN mart_behavior_structured_event_ai_gateway_flattened.enabled_by_duo_add_on = 'Duo Core'
                              THEN mart_behavior_structured_event_ai_gateway_flattened.gitlab_global_user_id 
                              ELSE NULL END))
            ELSE ZEROIFNULL(COUNT(DISTINCT CASE WHEN mart_behavior_structured_event_ai_gateway_flattened.enabled_by_duo_add_on != 'Duo Core'
                              THEN mart_behavior_structured_event_ai_gateway_flattened.gitlab_global_user_id 
                              ELSE NULL END))
        END                                                                                                      AS duo_active_users,
        ZEROIFNULL(duo_active_users / NULLIF(paid_duo_seats, 0))                                                 AS pct_usage_seat_utilization,
        IFF(pct_usage_seat_utilization > 1, 1, pct_usage_seat_utilization)                                       AS standard_pct_usage_seat_utilization,
        ZEROIFNULL(count_seats_assigned / NULLIF(paid_duo_seats, 0))                                             AS pct_assignment_seat_utilization,
        IFF(pct_assignment_seat_utilization > 1, 1, pct_assignment_seat_utilization)                             AS standard_pct_assignment_seat_utilization,
        COALESCE(combined_duo_info.is_oss_or_edu_rate_plan, FALSE)                                               AS is_oss_or_edu_rate_plan,
        combined_duo_info.account_owner,
        combined_duo_info.parent_crm_account_geo,
        combined_duo_info.parent_crm_account_sales_segment,
        combined_duo_info.parent_crm_account_industry,
        combined_duo_info.technical_account_manager,
        combined_duo_info.turn_on_cloud_licensing,
        combined_duo_info.license_type,
        ARRAY_AGG(DISTINCT combined_duo_info.product_entity_id) 
            WITHIN GROUP (ORDER BY combined_duo_info.product_entity_id)                                          AS product_entity_array,
        COUNT(DISTINCT combined_duo_info.product_entity_id)                                                      AS count_product_entities
    FROM combined_duo_info
    LEFT JOIN duo_seat_assignments
        ON TO_CHAR(combined_duo_info.product_entity_id) = TO_CHAR(duo_seat_assignments.product_entity_id)
        AND combined_duo_info.reporting_month = duo_seat_assignments.reporting_month
        AND combined_duo_info.add_on_name = duo_seat_assignments.add_on_type
        AND duo_seat_assignments.product_entity_id IS NOT NULL
    LEFT JOIN mart_behavior_structured_event_ai_gateway_flattened
        ON combined_duo_info.product_entity_id = mart_behavior_structured_event_ai_gateway_flattened.enabled_by_product_entity_id
        AND combined_duo_info.product_deployment = mart_behavior_structured_event_ai_gateway_flattened.enabled_by_product_deployment_type
        AND combined_duo_info.reporting_month = DATE_TRUNC(MONTH, mart_behavior_structured_event_ai_gateway_flattened.behavior_date)::DATE
    GROUP BY ALL

)

SELECT * 
FROM final
