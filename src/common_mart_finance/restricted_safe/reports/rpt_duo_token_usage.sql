{{ config(
    materialized='table',
    tags=["mnpi_exception", "finance"]
) }}

{{ simple_cte([
    ('mart_behavior_structured_event', 'mart_behavior_structured_event'),
    ('mart_behavior_structured_event_ai_gateway_flattened', 'mart_behavior_structured_event_ai_gateway_flattened'),
    ('rpt_duo_license_utilization_monthly', 'rpt_duo_license_utilization_monthly'),
    ('dim_crm_account', 'dim_crm_account')
]) }},

token_aggregation AS (

  SELECT
    DATE_TRUNC('MONTH', behavior_date) AS month,
    event_action,
    delivery_type,
    ''                                 AS feature_enablement_type,                              -- do not use - data is incomplete, but leaving column name to avoid breaking any dependencies
    model_provider,
    model_engine,
    model_name,
    gsc_correlation_id,                                         -- Keep this for joining to enriched data
    gitlab_global_user_id,
    SUM(input_tokens)                  AS input_tokens,
    SUM(output_tokens)                 AS output_tokens,
    SUM(total_tokens)                  AS total_tokens
  FROM mart_behavior_structured_event
  WHERE event_action LIKE 'token_usage%'
  {{ dbt_utils.group_by(9) }}

),

enriched_tokens AS (

  SELECT
    token_aggregation.month,
    token_aggregation.event_action,
    token_aggregation.delivery_type,
    token_aggregation.feature_enablement_type,
    token_aggregation.model_provider,
    token_aggregation.model_engine,
    token_aggregation.model_name,
    token_aggregation.gsc_correlation_id,
    token_aggregation.gitlab_global_user_id,
    token_aggregation.input_tokens,
    token_aggregation.output_tokens,
    token_aggregation.total_tokens,
    MAX(mart_behavior_structured_event_ai_gateway_flattened.enabled_by_duo_category)                     AS enabled_by_duo_category, -- Including MAX() to avoid duplicating token granularity
    MAX(mart_behavior_structured_event_ai_gateway_flattened.is_paid_duo)                                 AS is_paid_duo,
    MAX(mart_behavior_structured_event_ai_gateway_flattened.is_internal_usage_any)                       AS is_internal_usage_any,
    MAX(mart_behavior_structured_event_ai_gateway_flattened.enabled_by_dim_crm_account_id_at_event_time) AS dim_crm_account_id,
    MAX(mart_behavior_structured_event_ai_gateway_flattened.enabled_by_product_tier)                     AS tier
  FROM token_aggregation
  LEFT JOIN mart_behavior_structured_event_ai_gateway_flattened
    ON token_aggregation.gsc_correlation_id = mart_behavior_structured_event_ai_gateway_flattened.gsc_correlation_id
  {{ dbt_utils.group_by(12) }}
-- Uniqueness test passed at month, event_action, delivery_type, model_provider, model_engine, model_name, gsc_correlation_id, gitlab_global_user_id grain

),

aggregated_report AS (

  SELECT
    month,
    event_action,
    delivery_type,
    feature_enablement_type,                                    -- do not use - data is incomplete, but leaving column to avoid breaking any dependencies
    model_provider,
    model_engine,
    model_name,
    enabled_by_duo_category,
    is_paid_duo,
    is_internal_usage_any,
    dim_crm_account_id,
    tier,
    SUM(input_tokens)                     AS input_tokens,
    SUM(output_tokens)                    AS output_tokens,
    SUM(total_tokens)                     AS total_tokens,
    COUNT(DISTINCT gitlab_global_user_id) AS active_users
  FROM enriched_tokens
  {{ dbt_utils.group_by(12) }}
  HAVING active_users > 0

),

license_data AS (

  SELECT
    rpt_duo_license_utilization_monthly.dim_crm_account_id,
    rpt_duo_license_utilization_monthly.dim_parent_crm_account_id,
    rpt_duo_license_utilization_monthly.reporting_month,
    SUM(rpt_duo_license_utilization_monthly.paid_duo_seats) AS duo_pro_enterprise_q_seats
  FROM rpt_duo_license_utilization_monthly
  WHERE rpt_duo_license_utilization_monthly.add_on_name != 'GitLab Duo Core'                      -- Excluding tier-enabled feature reporting to avoid duplicate counting of licensed seats
  {{ dbt_utils.group_by(3) }}

),

final AS (

  SELECT
    aggregated_report.month,
    aggregated_report.event_action,
    aggregated_report.delivery_type,
    aggregated_report.feature_enablement_type,
    aggregated_report.model_provider,
    aggregated_report.model_engine,
    aggregated_report.model_name,
    aggregated_report.enabled_by_duo_category,
    aggregated_report.is_paid_duo,
    aggregated_report.is_internal_usage_any,
    aggregated_report.dim_crm_account_id,
    aggregated_report.tier,
    aggregated_report.input_tokens,
    aggregated_report.output_tokens,
    aggregated_report.total_tokens,
    aggregated_report.active_users,
    MAX(license_data.duo_pro_enterprise_q_seats) AS duo_pro_enterprise_q_seats, -- only includes add on subscriptions - using max to avoid duplication
    MAX(dim_crm_account.parent_crm_account_name) AS parent_crm_account_name -- using max to avoid duplication
  FROM aggregated_report
  LEFT JOIN license_data
    ON aggregated_report.dim_crm_account_id = license_data.dim_crm_account_id
      AND aggregated_report.month = license_data.reporting_month
  LEFT JOIN dim_crm_account
    ON license_data.dim_parent_crm_account_id = dim_crm_account.dim_parent_crm_account_id
  {{ dbt_utils.group_by(16) }}

)

SELECT *
FROM final
