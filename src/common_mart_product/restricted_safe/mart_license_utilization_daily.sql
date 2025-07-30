{{ simple_cte([
    ('fct_license_utilization', 'fct_license_utilization'),
    ('customers_db_trials', 'customers_db_trials'),
    ('prep_namespace_order_trial', 'prep_namespace_order_trial'),
    ('fct_trial', 'fct_trial'),
    ('dim_namespace','dim_namespace'),
    ('dim_subscription','dim_subscription'),
    ('dim_product_detail','dim_product_detail'),
    ('map_installation_subscription_product', 'map_installation_subscription_product'),
    ('dim_host_instance_type', 'dim_host_instance_type'),
    ('dim_product_tier', 'dim_product_tier')
])}},

dotcom_duo_trials AS (

  SELECT DISTINCT
    dim_namespace.dim_namespace_id,
    CASE
      WHEN prep_namespace_order_trial.trial_type = 2
        THEN 'GitLab Duo Pro'
      WHEN prep_namespace_order_trial.trial_type IN (3,5,6)
        THEN 'GitLab Duo Enterprise'
    END                                                               AS product_category,
    prep_namespace_order_trial.order_start_date                       AS trial_start_date,
    MAX(fct_trial.trial_end_date)                                     AS trial_end_date
  FROM prep_namespace_order_trial
  LEFT JOIN fct_trial
    ON prep_namespace_order_trial.dim_namespace_id = fct_trial.dim_namespace_id
    AND prep_namespace_order_trial.order_start_date = fct_trial.trial_start_date
    AND fct_trial.product_rate_plan_id LIKE '%duo%'
  INNER JOIN dim_namespace
    ON prep_namespace_order_trial.dim_namespace_id = dim_namespace.dim_namespace_id
  WHERE dim_namespace.namespace_creator_is_blocked = FALSE 
    AND dim_namespace.namespace_is_ultimate_parent = TRUE
  GROUP BY ALL

),

sm_dedicated_duo_trials AS (

  SELECT DISTINCT
    map_installation_subscription_product.dim_installation_id::VARCHAR                            AS dim_installation_id,
    map_installation_subscription_product.dim_subscription_id,
    TRIM(SPLIT_PART(SPLIT_PART(dim_product_detail.product_rate_plan_category, '(', 1), '- ', 2))  AS product_category,
    customers_db_trials.start_date::DATE                                                          AS trial_start_date,
    customers_db_trials.end_date::DATE                                                            AS trial_end_date,
  FROM customers_db_trials
  LEFT JOIN dim_product_detail
    ON customers_db_trials.product_rate_plan_id = dim_product_detail.product_rate_plan_id
  LEFT JOIN dim_subscription
    ON customers_db_trials.subscription_name = dim_subscription.subscription_name
     AND dim_subscription.subscription_version = 1
  INNER JOIN map_installation_subscription_product
    ON dim_subscription.dim_subscription_id_original = map_installation_subscription_product.dim_subscription_id_original
      AND map_installation_subscription_product.date_actual BETWEEN customers_db_trials.start_date AND customers_db_trials.end_date

)

, production_instance_tagging AS (

  SELECT *
  FROM dim_host_instance_type
  QUALIFY ROW_NUMBER() OVER (PARTITION BY COALESCE(instance_uuid, namespace_id) ORDER BY instance_type_ordering_field ASC, health_score_ordering_field ASC) = 1
  -- prefer production instances included in the health scoring

)

, joined AS (

  SELECT 
    fct_license_utilization.*,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM sm_dedicated_duo_trials
        WHERE sm_dedicated_duo_trials.dim_installation_id = fct_license_utilization.dim_installation_id
          AND fct_license_utilization.report_date BETWEEN sm_dedicated_duo_trials.trial_start_date AND sm_dedicated_duo_trials.trial_end_date
          AND fct_license_utilization.license_users > 0 
          AND sm_dedicated_duo_trials.product_category = 'GitLab Duo Pro'
      ) OR EXISTS (
        SELECT 1
        FROM dotcom_duo_trials
        WHERE dotcom_duo_trials.dim_namespace_id = fct_license_utilization.dim_namespace_id
          AND fct_license_utilization.report_date BETWEEN dotcom_duo_trials.trial_start_date AND dotcom_duo_trials.trial_end_date
          AND fct_license_utilization.license_users > 0 
          AND dotcom_duo_trials.product_category = 'GitLab Duo Pro'
      ) 
        THEN TRUE
      ELSE FALSE
    END                                                                                 AS is_duo_pro_trial,

    CASE
      WHEN EXISTS (
        SELECT 1
        FROM sm_dedicated_duo_trials
        WHERE sm_dedicated_duo_trials.dim_installation_id = fct_license_utilization.dim_installation_id
          AND fct_license_utilization.report_date BETWEEN sm_dedicated_duo_trials.trial_start_date AND sm_dedicated_duo_trials.trial_end_date
          AND fct_license_utilization.license_users > 0 
          AND sm_dedicated_duo_trials.product_category = 'GitLab Duo Enterprise'
      ) OR EXISTS (
        SELECT 1
        FROM dotcom_duo_trials
        WHERE dotcom_duo_trials.dim_namespace_id = fct_license_utilization.dim_namespace_id
          AND fct_license_utilization.report_date BETWEEN dotcom_duo_trials.trial_start_date AND dotcom_duo_trials.trial_end_date
          AND fct_license_utilization.license_users > 0 
          AND dotcom_duo_trials.product_category = 'GitLab Duo Enterprise'
      )
        THEN TRUE
      ELSE FALSE
    END                                                                               AS is_duo_enterprise_trial,
    CASE 
      WHEN production_instance_tagging.instance_type = 'Production' AND 
         ROW_NUMBER() OVER (
           PARTITION BY fct_license_utilization.report_date, fct_license_utilization.dim_subscription_id_original 
           ORDER BY fct_license_utilization.billable_users DESC NULLS LAST
         ) = 1
        THEN TRUE
      ELSE FALSE
    END                                                                               AS is_max_billable_instance_for_subscription,
    dim_namespace.namespace_is_internal,
    production_instance_tagging.instance_type,
    production_instance_tagging.instance_type_ordering_field,
    production_instance_tagging.health_score_ordering_field,
    dim_product_tier.product_tier_name,
    dim_product_tier.product_delivery_type

  FROM fct_license_utilization
  LEFT JOIN dim_namespace
    ON fct_license_utilization.dim_namespace_id = dim_namespace.dim_namespace_id
  LEFT JOIN production_instance_tagging
    ON fct_license_utilization.dim_installation_id = production_instance_tagging.dim_installation_id
      OR fct_license_utilization.dim_namespace_id::VARCHAR = production_instance_tagging.namespace_id::VARCHAR
  LEFT JOIN dim_product_tier
    ON fct_license_utilization.dim_product_tier_id = dim_product_tier.dim_product_tier_id
)

SELECT
  joined.report_date,

  joined.dim_installation_id,
  joined.dim_namespace_id,
  joined.dim_subscription_id,
  joined.dim_subscription_id_original,
  joined.dim_crm_account_id,
  joined.dim_product_tier_id,

  joined.assignable_feature_set,
  joined.product_deployment_type,
  joined.product_tier_name,
  joined.product_delivery_type,

  joined.is_duo_pro_trial,
  joined.is_duo_enterprise_trial,
  joined.namespace_is_internal,
  joined.is_max_billable_instance_for_subscription,
  joined.instance_type,
  joined.instance_type_ordering_field,
  joined.health_score_ordering_field,

  joined.license_users_source,
  joined.billable_users_source,

  joined.license_users,
  joined.billable_users,
  joined.utilization_rate

FROM joined
