{{ config(
    materialized='table',
    tags=["product","mnpi_exception"]
) }}

WITH saas_ping_namespace_prep AS ( -- Ping metrics for .com deployments
    SELECT
        fct.* EXCLUDE (metric_value, ping_namespace_metric_pk, ping_metric_id, dim_ping_date_id),
        fct.metric_value,
        dim_ping_metric.time_frame,
        dim_ping_metric.group_name,
        dim_ping_metric.stage_name,
        dim_ping_metric.section_name,
        dim_ping_metric.is_smau,
        dim_ping_metric.is_gmau,
        dim_ping_metric.is_paid_gmau,
        dim_ping_metric.is_umau,
        dim_namespace.ultimate_parent_namespace_id                      AS product_entity_id,
        'Ultimate Parent Namespace ID'                                  AS product_entity_type,
        DATE_TRUNC('month', fct.ping_created_at)                        AS ping_created_date_month
    FROM {{ ref('fct_ping_namespace_metric' )}} fct
    INNER JOIN {{ ref('dim_ping_metric' )}} dim_ping_metric
        ON fct.metrics_path = dim_ping_metric.metrics_path
        AND dim_ping_metric.time_frame IN ('28d', 'all') -- To stay consistent with Self-Managed, this only looks at 28-day and all-time metrics
    INNER JOIN {{ ref('dim_namespace' )}} -- To pull in Ultimate Namespace ID
        ON CAST(dim_namespace.dim_namespace_id AS VARCHAR) = CAST(fct.dim_namespace_id AS VARCHAR)
        AND dim_namespace.namespace_is_internal = FALSE
    WHERE DATEDIFF('month', ping_created_at, CURRENT_DATE) <= 25
    -- To pull the latest record per month per metric
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fct.dim_namespace_id, fct.metrics_path, ping_created_date_month 
        ORDER BY ping_created_at DESC
    ) = 1
), 

saas_ping_namespace_metric_values AS ( -- For Ultimate Parent Namespaces with over 1 record per metric, this pulls the MAX value for that month (this occurs for .02% of records)
    SELECT
        *,
        LAG(metric_value,1) OVER (PARTITION BY dim_namespace_id, metrics_path ORDER BY ping_created_date_month ASC)         AS previous_month_metric_value,
        IFF(IFNULL(metric_value,0) < IFNULL(previous_month_metric_value,0), 0, metric_value - previous_month_metric_value)  AS monthly_change_in_metric_value,
        MAX(IFF(is_umau = TRUE, metric_value, NULL)) OVER (PARTITION BY product_entity_id, ping_created_date_month)         AS umau_value
    FROM saas_ping_namespace_prep
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_entity_id, ping_created_date_month, metrics_path 
        ORDER BY metric_value DESC
    ) = 1
), 

saas_ping_namespace AS (

    SELECT 
        * EXCLUDE (dim_namespace_id),
        CASE time_frame
            WHEN '28d' THEN metric_value
            WHEN 'all' THEN monthly_change_in_metric_value
            ELSE NULL
        END                                                                                                                  AS monthly_metric_value
    FROM saas_ping_namespace_metric_values
),

namespace_and_plan AS ( -- Finds the monthly plan name associated with the namespace. namespace_daily is used since namespace_monthly does not include the latest month's CRM account ID
    SELECT DISTINCT
        dim_ultimate_parent_namespace_id,
        event_calendar_month,
        dim_latest_subscription_id,
        dim_latest_product_tier_id,
        plan_name_at_event_date,
        dim_crm_account_id,
        plan_was_paid_at_event_date
    FROM {{ ref('mart_event_namespace_daily' )}}
    WHERE namespace_is_internal = FALSE
        AND DATEDIFF('month', event_calendar_month, CURRENT_DATE) <= 25
        AND is_umau = TRUE -- Ensure a minimum-level of activity on the namespace
    -- Finds the latest record per month
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event_calendar_month, dim_ultimate_parent_namespace_id 
        ORDER BY event_date DESC
    ) = 1
), 

crm_and_subscriptions AS ( -- Connects namespace, subscription, and CRM data
    SELECT
        namespace_and_plan.dim_ultimate_parent_namespace_id,
        namespace_and_plan.event_calendar_month,
        plan_name_at_event_date                                   AS ping_product_tier,
        crm.crm_account_name,
        namespace_and_plan.dim_latest_subscription_id             AS latest_subscription_id,
        dim_subscription.dim_subscription_id_original,
        crm.dim_crm_account_id,
        'GitLab.com'                                              AS ping_deployment_type,
        namespace_and_plan.plan_was_paid_at_event_date            AS is_paid_subscription,
        crm.parent_crm_account_name,
        crm.parent_crm_account_industry,
        crm.parent_crm_account_sales_segment,
        NULL                                                      AS major_minor_version_id,
        'GitLab.com'                                              AS major_minor_version,
        parent_crm_account_region,
        parent_crm_account_geo
    FROM namespace_and_plan
    LEFT JOIN {{ ref('dim_crm_account' )}} crm
        ON crm.dim_crm_account_id = namespace_and_plan.dim_crm_account_id
    LEFT JOIN {{ ref('dim_subscription' )}} 
        ON dim_subscription.dim_subscription_id = latest_subscription_id
), 

dotcom AS ( -- ties all .com data together
    SELECT 
        -- Date Attributes
        ping_created_date_month,
        -- CRM attributes
        dim_crm_account_id,
        crm_account_name,
        parent_crm_account_name,
        parent_crm_account_industry,
        parent_crm_account_sales_segment,
        parent_crm_account_region,
        parent_crm_account_geo,
        -- Subscription Attributes
        CAST(latest_subscription_id AS VARCHAR)                   AS latest_subscription_id,
        dim_subscription_id_original,
        is_paid_subscription,
        -- Entity & Version information
        CAST(product_entity_id AS VARCHAR)                        AS product_entity_id,
        product_entity_type,
        ping_product_tier,
        ping_deployment_type,
        major_minor_version_id,
        major_minor_version,
        -- Metric information
        metrics_path,
        monthly_metric_value,
        metric_value,
        time_frame,
        group_name,
        stage_name,
        section_name,
        is_smau,
        is_gmau,
        is_paid_gmau,
        is_umau,
        umau_value
    FROM saas_ping_namespace
    INNER JOIN crm_and_subscriptions 
        ON CAST(crm_and_subscriptions.dim_ultimate_parent_namespace_id AS VARCHAR) = CAST(saas_ping_namespace.product_entity_id AS VARCHAR)
        AND crm_and_subscriptions.event_calendar_month = saas_ping_namespace.ping_created_date_month
), 

self_managed AS ( -- ties all self-managed data together
    SELECT 
        -- Date Attributes
        ping.ping_created_date_month,
        -- CRM attributes
        ping.dim_crm_account_id,
        ping.crm_account_name,
        dim_crm_account.parent_crm_account_name,
        dim_crm_account.parent_crm_account_industry,
        dim_crm_account.parent_crm_account_sales_segment,
        dim_crm_account.parent_crm_account_region,
        dim_crm_account.parent_crm_account_geo,
        -- Subscription Attributes
        CAST(latest_subscription_id AS VARCHAR)                   AS latest_subscription_id,
        dim_subscription.dim_subscription_id_original,
        is_paid_subscription,
        -- Entity & Version information
        CAST(dim_installation_id AS VARCHAR)                      AS product_entity_id,
        'Installation ID'                                         AS product_entity_type,
        CASE is_trial -- CASE statement to match the product tier naming convention between dotcom and Self-Managed
            WHEN TRUE THEN CONCAT(LOWER(ping_product_tier), '_', 'trial')
            WHEN FALSE THEN LOWER(ping_product_tier)
        END                                                       AS ping_product_tier,
        ping_deployment_type,
        major_minor_version_id,
        major_minor_version,
        -- Metric information
        metrics_path,
        monthly_metric_value,
        metric_value,
        time_frame,
        group_name,
        stage_name,
        section_name,
        is_smau,
        is_gmau,
        is_paid_gmau,
        is_umau,
        ping.umau_value
    FROM {{ ref('mart_ping_instance_metric_monthly' )}} ping
    LEFT JOIN {{ ref('dim_crm_account' )}} 
        ON dim_crm_account.dim_crm_account_id = ping.dim_crm_account_id
    LEFT JOIN {{ ref('dim_subscription' )}} 
        ON dim_subscription.dim_subscription_id = latest_subscription_id
    WHERE DATEDIFF('month', ping_created_date_month, CURRENT_DATE) <= 25
        AND is_internal = FALSE
        AND metric_value > 0
        AND umau_value > 0
        AND is_last_ping_of_month = TRUE
    GROUP BY ALL
    
), unioned AS (
    
    SELECT * FROM self_managed
    
    UNION ALL
    
    SELECT * FROM dotcom

), final AS (

SELECT
    -- Primary Key 
    {{ dbt_utils.generate_surrogate_key(['ping_created_date_month', 'metrics_path', 'product_entity_id']) }}  AS entity_metric_monthly_pk,
    * 
FROM unioned

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@dpeterson1",
    updated_by="@dpeterson1",
    created_date="2025-05-14",
    updated_date="2025-06-05"
) }}
