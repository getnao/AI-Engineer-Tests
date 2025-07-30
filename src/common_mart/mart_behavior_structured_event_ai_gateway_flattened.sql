{{ config(
    materialized="table",
    tags=["product", "mnpi_exception"],
    cluster_by=['behavior_at::DATE']
) }}

{{ simple_cte([
    ('fct_behavior_structured_event', 'fct_behavior_structured_event'),
    ('dim_behavior_event', 'dim_behavior_event'),
    ('dim_installation', 'dim_installation'),
    ('dim_namespace', 'dim_namespace'),
    ('dim_app_release_major_minor', 'dim_app_release_major_minor'),
    ('map_namespace_subscription_product', 'map_namespace_subscription_product'),
    ('map_installation_subscription_product','map_installation_subscription_product'),
    ('dim_product_detail', 'dim_product_detail'),
    ('dim_crm_account', 'dim_crm_account'),
    ('prep_trial', 'prep_trial'),
    ('dim_subscription', 'dim_subscription'),
    ('customers_db_trials', 'customers_db_trials'),
    ('dim_ai_gateway_unit_primitives', 'dim_ai_gateway_unit_primitives')
    ])
}},

prep_namespace_order_trial AS (

  SELECT
    trial_type,
    order_start_date,
    dim_namespace_id,
    CASE
      WHEN prep_namespace_order_trial.trial_type = 2
        THEN 'Duo Pro Trial'
      WHEN prep_namespace_order_trial.trial_type IN (3, 5, 6)
        THEN 'Duo Enterprise Trial'
    END AS product_rate_plan_category_general
  FROM {{ ref('prep_namespace_order_trial') }}

),

up_events AS (

  SELECT
    fct_behavior_structured_event.* EXCLUDE (feature_category),
    fct_behavior_structured_event.behavior_at::DATE AS behavior_date,
    fct_behavior_structured_event.feature_category  AS feature_category_at_event_time,
    dim_behavior_event.event,
    dim_behavior_event.event_name,
    dim_behavior_event.platform,
    dim_behavior_event.environment,
    dim_behavior_event.event_category,
    dim_behavior_event.event_action,
    dim_behavior_event.event_property,
    dim_behavior_event.unit_primitive,
    dim_ai_gateway_unit_primitives.feature_category,
    dim_ai_gateway_unit_primitives.engineering_group,
    dim_ai_gateway_unit_primitives.backend_services
  FROM fct_behavior_structured_event
  LEFT JOIN dim_behavior_event
    ON fct_behavior_structured_event.dim_behavior_event_sk = dim_behavior_event.dim_behavior_event_sk
  LEFT JOIN dim_ai_gateway_unit_primitives
    ON dim_behavior_event.unit_primitive = dim_ai_gateway_unit_primitives.unit_primitive_name

  /*
  Filters:
  - first date after events were implemented and pseudonymization was fixed in https://gitlab.com/gitlab-org/analytics-section/analytics-instrumentation/snowplow-pseudonymization/-/merge_requests/27
  - event_action indicates it is a unit primitive
  - event occurred in AI Gateway
  */
  WHERE behavior_at >= '2024-08-03'
    AND event_action LIKE 'request_%'
    AND app_id = 'gitlab_ai_gateway'

),

sm_dedicated_duo_trials AS (

  SELECT
    customers_db_trials.subscription_name,
    customers_db_trials.start_date                                                               AS trial_start_date,
    customers_db_trials.end_date                                                                 AS trial_end_date,
    customers_db_trials.product_rate_plan_id,
    TRIM(SPLIT_PART(SPLIT_PART(dim_product_detail.product_rate_plan_category, '(', 1), '- ', 2)) AS product_category_short,
    CASE
      WHEN product_category_short = 'GitLab Duo Pro'
        THEN 'Duo Pro Trial'
      WHEN product_category_short = 'GitLab Duo Enterprise'
        THEN 'Duo Enterprise Trial'
      WHEN product_category_short = 'GitLab Duo with Amazon Q'
        THEN 'Duo with Amazon Q Trial'
    END                                                                                          AS product_rate_plan_category_general,
    dim_product_detail.product_rate_plan_name,
    dim_subscription.dim_subscription_id_original,
    customers_db_trials.subscription_name                                                        AS enabled_by_sm_dedicated_duo_trial_subscription_name
  FROM customers_db_trials
  LEFT JOIN dim_product_detail
    ON customers_db_trials.product_rate_plan_id = dim_product_detail.product_rate_plan_id
  LEFT JOIN dim_subscription
    ON customers_db_trials.subscription_name = dim_subscription.subscription_name
      AND dim_subscription.subscription_version = 1
  WHERE dim_product_detail.product_rate_plan_name ILIKE '%duo%'

),

dotcom_duo_trials AS (

  SELECT DISTINCT
    dim_namespace.dim_namespace_id,
    ARRAY_UNIQUE_AGG(product_rate_plan_category_general) AS product_rate_plan_category_general,
    MIN(prep_namespace_order_trial.order_start_date)     AS earliest_trial_start_date,
    MAX(prep_trial.trial_end_date)                       AS latest_trial_end_date
  FROM prep_namespace_order_trial
  LEFT JOIN prep_trial
    ON prep_namespace_order_trial.dim_namespace_id = prep_trial.dim_namespace_id
      AND prep_namespace_order_trial.order_start_date = prep_trial.trial_start_date
      AND prep_trial.product_rate_plan_id LIKE '%duo%'
  INNER JOIN dim_namespace
    ON prep_namespace_order_trial.dim_namespace_id = dim_namespace.dim_namespace_id
  WHERE prep_namespace_order_trial.trial_type IN (2, 3, 5, 6)
  GROUP BY 1

),

flattened AS (

  SELECT
    up_events.*,
    flattened_namespace.value::VARCHAR AS enabled_by_namespace_id
  FROM up_events,
    LATERAL FLATTEN(input => TRY_PARSE_JSON(up_events.gsc_feature_enabled_by_namespace_ids), outer => TRUE) AS flattened_namespace

),

flattened_with_installation_id AS (

  SELECT
    flattened.*,
    dim_installation.dim_installation_id,
    dim_installation.product_delivery_type                              AS enabled_by_product_delivery_type,
    dim_installation.product_deployment_type                            AS enabled_by_product_deployment_type,
    REGEXP_SUBSTR(flattened.gsc_instance_version, '(.*)[.]', 1, 1, 'e') AS major_minor_version,
    dim_installation.is_internal
  FROM flattened
  LEFT JOIN dim_installation
    ON flattened.dim_instance_id = dim_installation.dim_instance_id
      AND flattened.host_name = dim_installation.host_name

),

installation_sub_product AS (

  SELECT
    map_installation_subscription_product.date_actual,
    map_installation_subscription_product.dim_subscription_id,
    map_installation_subscription_product.dim_subscription_id_original,
    map_installation_subscription_product.dim_installation_id,
    map_installation_subscription_product.dim_crm_account_id,
    map_installation_subscription_product.is_positive_mrr,
    dim_crm_account.crm_account_name,
    dim_crm_account.account_owner,
    dim_crm_account.parent_crm_account_geo,
    dim_crm_account.parent_crm_account_sales_segment,
    dim_crm_account.parent_crm_account_industry,
    dim_crm_account.technical_account_manager,
    dim_crm_account.parent_crm_account_region,
    dim_product_detail.*,
    TRIM(SPLIT_PART(SPLIT_PART(dim_product_detail.product_rate_plan_category, '(', 1), '- ', 2)) AS product_rate_plan_category_general
  FROM map_installation_subscription_product
  LEFT JOIN dim_product_detail
    ON map_installation_subscription_product.dim_product_detail_id = dim_product_detail.dim_product_detail_id
  LEFT JOIN dim_crm_account
    ON map_installation_subscription_product.dim_crm_account_id = dim_crm_account.dim_crm_account_id

),

installation_subscription AS (

  SELECT DISTINCT
    date_actual,
    dim_installation_id,
    ARRAY_UNIQUE_AGG(installation_sub_product.dim_subscription_id)                AS dim_subscription_ids,
    ARRAY_UNIQUE_AGG(installation_sub_product.dim_subscription_id_original)       AS dim_subscription_ids_original,
    ARRAY_UNIQUE_AGG(installation_sub_product.dim_crm_account_id)                 AS dim_crm_account_ids,
    ARRAY_UNIQUE_AGG(installation_sub_product.crm_account_name)                   AS crm_account_names,
    ARRAY_UNIQUE_AGG(installation_sub_product.account_owner)                      AS account_owners,
    ARRAY_UNIQUE_AGG(installation_sub_product.parent_crm_account_geo)             AS parent_crm_account_geos,
    ARRAY_UNIQUE_AGG(installation_sub_product.parent_crm_account_sales_segment)   AS parent_crm_account_sales_segments,
    ARRAY_UNIQUE_AGG(installation_sub_product.parent_crm_account_industry)        AS parent_crm_account_industries,
    ARRAY_UNIQUE_AGG(installation_sub_product.technical_account_manager)          AS technical_account_managers,
    ARRAY_UNIQUE_AGG(installation_sub_product.parent_crm_account_region)          AS parent_crm_account_regions,
    ARRAY_UNIQUE_AGG(installation_sub_product.product_rate_plan_category_general) AS product_categories,
    ARRAY_UNIQUE_AGG(installation_sub_product.product_tier_name_short)            AS product_tier_names,
    MAX(installation_sub_product.is_oss_or_edu_rate_plan)                         AS oss_or_edu_rate_plans
  FROM installation_sub_product
  WHERE product_category = 'Base Products'
    AND charge_type = 'Recurring'
    AND is_licensed_user = TRUE
  {{ dbt_utils.group_by(n=2) }}

),

add_on_installation_sub_product AS (

  SELECT DISTINCT
    date_actual,
    dim_installation_id,
    ARRAY_UNIQUE_AGG(installation_sub_product.dim_subscription_id)                AS add_on_dim_subscription_ids,
    ARRAY_UNIQUE_AGG(installation_sub_product.dim_crm_account_id)                 AS add_on_dim_crm_account_ids,
    ARRAY_UNIQUE_AGG(installation_sub_product.crm_account_name)                   AS add_on_crm_account_names,
    ARRAY_UNIQUE_AGG(installation_sub_product.product_rate_plan_category_general) AS add_on_product_categories
  FROM installation_sub_product
   WHERE (
        product_category = 'Add On Services' 
          OR SPLIT_PART(product_rate_plan_category, ' - ', 2) = 'GitLab Duo with Amazon Q' --specified because this bundled add-on is sold under product_category = 'Base Products' (issue: https://gitlab.com/gitlab-data/analytics/-/issues/23730)
        )
    AND charge_type = 'Recurring'
    AND is_licensed_user = TRUE
  {{ dbt_utils.group_by(n=2) }}

),

add_on_sm_trial AS (

  SELECT
    date_actual,
    dim_installation_id,
    ARRAY_UNIQUE_AGG(sm_dedicated_duo_trials.product_rate_plan_category_general) AS add_on_trial_product_categories
  FROM installation_sub_product
  LEFT JOIN sm_dedicated_duo_trials
    ON installation_sub_product.dim_subscription_id_original = sm_dedicated_duo_trials.dim_subscription_id_original
      AND installation_sub_product.date_actual BETWEEN sm_dedicated_duo_trials.trial_start_date AND sm_dedicated_duo_trials.trial_end_date
  {{ dbt_utils.group_by(n=2) }}

),

namespace_sub_product AS (

  SELECT
    map_namespace_subscription_product.date_actual,
    map_namespace_subscription_product.dim_subscription_id,
    map_namespace_subscription_product.dim_subscription_id_original,
    map_namespace_subscription_product.dim_namespace_id,
    map_namespace_subscription_product.dim_crm_account_id,
    map_namespace_subscription_product.is_positive_mrr,
    dim_crm_account.crm_account_name,
    dim_crm_account.account_owner,
    dim_crm_account.parent_crm_account_geo,
    dim_crm_account.parent_crm_account_sales_segment,
    dim_crm_account.parent_crm_account_industry,
    dim_crm_account.technical_account_manager,
    dim_crm_account.parent_crm_account_region,
    dim_product_detail.*,
    TRIM(SPLIT_PART(SPLIT_PART(dim_product_detail.product_rate_plan_category, '(', 1), '- ', 2)) AS product_rate_plan_category_general
  FROM map_namespace_subscription_product
  LEFT JOIN dim_product_detail
    ON map_namespace_subscription_product.dim_product_detail_id = dim_product_detail.dim_product_detail_id
  LEFT JOIN dim_crm_account
    ON map_namespace_subscription_product.dim_crm_account_id = dim_crm_account.dim_crm_account_id

),

namespace_subscription AS (

  SELECT DISTINCT
    date_actual,
    dim_namespace_id,
    ARRAY_UNIQUE_AGG(namespace_sub_product.dim_subscription_id)                AS enabled_by_dim_subscription_ids,
    ARRAY_UNIQUE_AGG(namespace_sub_product.dim_subscription_id_original)       AS enabled_by_dim_subscription_ids_original,
    ARRAY_UNIQUE_AGG(namespace_sub_product.dim_crm_account_id)                 AS enabled_by_dim_crm_account_ids,
    ARRAY_UNIQUE_AGG(namespace_sub_product.crm_account_name)                   AS enabled_by_crm_account_names,
    ARRAY_UNIQUE_AGG(namespace_sub_product.account_owner)                      AS enabled_by_account_owners,
    ARRAY_UNIQUE_AGG(namespace_sub_product.parent_crm_account_geo)             AS enabled_by_parent_crm_account_geos,
    ARRAY_UNIQUE_AGG(namespace_sub_product.parent_crm_account_sales_segment)   AS enabled_by_parent_crm_account_sales_segments,
    ARRAY_UNIQUE_AGG(namespace_sub_product.parent_crm_account_industry)        AS enabled_by_parent_crm_account_industries,
    ARRAY_UNIQUE_AGG(namespace_sub_product.technical_account_manager)          AS enabled_by_technical_account_managers,
    ARRAY_UNIQUE_AGG(namespace_sub_product.parent_crm_account_region)          AS enabled_by_parent_crm_account_regions,
    ARRAY_UNIQUE_AGG(namespace_sub_product.product_rate_plan_category_general) AS enabled_by_product_categories,
    ARRAY_UNIQUE_AGG(namespace_sub_product.product_tier_name_short)            AS enabled_by_product_tier_names,
    MAX(namespace_sub_product.is_oss_or_edu_rate_plan)                         AS enabled_by_oss_or_edu_rate_plan
  FROM namespace_sub_product
  WHERE product_category = 'Base Products'
    AND charge_type = 'Recurring'
    AND is_licensed_user = TRUE
  {{ dbt_utils.group_by(n=2) }}

),

add_on_namespace_sub_product AS (

 SELECT DISTINCT
   date_actual,
   dim_namespace_id,
   ARRAY_UNIQUE_AGG(namespace_sub_product.dim_subscription_id)                AS enabled_by_add_on_dim_subscription_ids,
   ARRAY_UNIQUE_AGG(namespace_sub_product.dim_crm_account_id)                 AS enabled_by_add_on_dim_crm_account_ids,
   ARRAY_UNIQUE_AGG(namespace_sub_product.crm_account_name)                   AS enabled_by_add_on_crm_account_names,
   ARRAY_UNIQUE_AGG(namespace_sub_product.product_rate_plan_category_general) AS enabled_by_add_on_product_categories
 FROM namespace_sub_product
 WHERE product_category = 'Add On Services' 
   AND charge_type = 'Recurring'
   AND is_licensed_user = TRUE
 {{ dbt_utils.group_by(n=2) }}


),

paid_duo_namespace AS (
  -- This CTE identifies namespaces with paid Duo subscriptions 
  SELECT DISTINCT
    namespace_sub_product.date_actual,
    namespace_sub_product.dim_namespace_id,
    TRUE AS has_paid_duo_namespace
  FROM namespace_sub_product
  WHERE
    namespace_sub_product.product_rate_plan_name ILIKE '%duo%'
    AND namespace_sub_product.is_positive_mrr
),

paid_duo_installation AS (
  -- This CTE identifies installations with paid Duo subscriptions 
  SELECT DISTINCT
    installation_sub_product.date_actual,
    installation_sub_product.dim_installation_id,
    TRUE AS has_paid_duo_installation
  FROM installation_sub_product
  WHERE
    installation_sub_product.product_rate_plan_name ILIKE '%duo%'
    AND installation_sub_product.is_positive_mrr

),

joined AS (

  SELECT
    -- primary key
    flattened_with_installation_id.behavior_structured_event_pk,

    -- foreign keys
    flattened_with_installation_id.dim_behavior_event_sk,
    dim_app_release_major_minor.dim_app_release_major_minor_sk,
    flattened_with_installation_id.dim_installation_id,
    flattened_with_installation_id.gsc_feature_enabled_by_namespace_ids,
    flattened_with_installation_id.enabled_by_namespace_id,
    dim_namespace.ultimate_parent_namespace_id                                                                                               AS enabled_by_ultimate_parent_namespace_id,

    -- dates
    flattened_with_installation_id.behavior_at,
    flattened_with_installation_id.behavior_date,

    -- degenerate dimensions
    flattened_with_installation_id.dim_instance_id,
    flattened_with_installation_id.unique_instance_id,
    flattened_with_installation_id.host_name,
    flattened_with_installation_id.is_internal                                                                                               AS enabled_by_internal_installation,
    dim_namespace.namespace_is_internal                                                                                                      AS enabled_by_internal_namespace,
    flattened_with_installation_id.enabled_by_product_delivery_type,
    flattened_with_installation_id.enabled_by_product_deployment_type,
    flattened_with_installation_id.gitlab_global_user_id,
    flattened_with_installation_id.app_id,
    flattened_with_installation_id.deployment_type,

    -- standard context attributes
    flattened_with_installation_id.contexts,
    flattened_with_installation_id.gitlab_standard_context,
    flattened_with_installation_id.gsc_environment,
    flattened_with_installation_id.gsc_source,
    flattened_with_installation_id.delivery_type,
    flattened_with_installation_id.gsc_correlation_id,
    flattened_with_installation_id.gsc_extra,
    flattened_with_installation_id.gsc_instance_version,
    dim_app_release_major_minor.major_minor_version                                                                                          AS enabled_by_major_minor_version_at_event_time,
    dim_app_release_major_minor.major_minor_version_num                                                                                      AS enabled_by_major_minor_version_num_at_event_time,
    flattened_with_installation_id.interface,
    flattened_with_installation_id.client_type,
    flattened_with_installation_id.client_name,
    flattened_with_installation_id.client_version,
    flattened_with_installation_id.feature_category_at_event_time,
    flattened_with_installation_id.gsc_is_gitlab_team_member,
    flattened_with_installation_id.feature_category,
    flattened_with_installation_id.engineering_group,
    flattened_with_installation_id.backend_services,
    flattened_with_installation_id.input_tokens,
    flattened_with_installation_id.output_tokens,
    flattened_with_installation_id.total_tokens,
    flattened_with_installation_id.model_engine,
    flattened_with_installation_id.model_name,
    flattened_with_installation_id.model_provider,
    flattened_with_installation_id.feature_enablement_type,

    -- user attributes
    flattened_with_installation_id.user_country,
    flattened_with_installation_id.user_timezone_name,
    flattened_with_installation_id.user_type,

    -- event attributes
    flattened_with_installation_id.event_value,
    flattened_with_installation_id.event_category,
    flattened_with_installation_id.event_action,
    flattened_with_installation_id.event_label,
    flattened_with_installation_id.clean_event_label,
    flattened_with_installation_id.event_property,
    flattened_with_installation_id.unit_primitive,

    -- customer ids/product information
    COALESCE(
      IFF(ARRAY_SIZE(namespace_subscription.enabled_by_dim_subscription_ids) = 0, NULL, namespace_subscription.enabled_by_dim_subscription_ids),
      IFF(ARRAY_SIZE(installation_subscription.dim_subscription_ids) = 0, NULL, installation_subscription.dim_subscription_ids)
    )                                                                                                                                        AS enabled_by_dim_subscription_ids_at_event_time,
    COALESCE(
      IFF(ARRAY_SIZE(namespace_subscription.enabled_by_dim_subscription_ids_original) = 0, NULL, namespace_subscription.enabled_by_dim_subscription_ids_original),
      IFF(ARRAY_SIZE(installation_subscription.dim_subscription_ids_original) = 0, NULL, installation_subscription.dim_subscription_ids_original)
    )                                                                                                                                        AS enabled_by_dim_subscription_ids_original_at_event_time,
    ARRAY_TO_STRING(
      COALESCE(
        IFF(ARRAY_SIZE(namespace_subscription.enabled_by_dim_crm_account_ids) = 0, NULL, namespace_subscription.enabled_by_dim_crm_account_ids),
        IFF(ARRAY_SIZE(installation_subscription.dim_crm_account_ids) = 0, NULL, installation_subscription.dim_crm_account_ids),
        IFF(ARRAY_SIZE(add_on_namespace_sub_product.enabled_by_add_on_dim_crm_account_ids) = 0, NULL, add_on_namespace_sub_product.enabled_by_add_on_dim_crm_account_ids),
        IFF(ARRAY_SIZE(add_on_installation_sub_product.add_on_dim_crm_account_ids) = 0, NULL, add_on_installation_sub_product.add_on_dim_crm_account_ids)
      ),
      ' ,'
    )                                                                                                                                        AS enabled_by_dim_crm_account_id_at_event_time,
    ARRAY_TO_STRING(
      COALESCE(
        IFF(ARRAY_SIZE(namespace_subscription.enabled_by_crm_account_names) = 0, NULL, namespace_subscription.enabled_by_crm_account_names),
        IFF(ARRAY_SIZE(installation_subscription.crm_account_names) = 0, NULL, installation_subscription.crm_account_names),
        IFF(ARRAY_SIZE(add_on_namespace_sub_product.enabled_by_add_on_crm_account_names) = 0, NULL, add_on_namespace_sub_product.enabled_by_add_on_crm_account_names),
        IFF(ARRAY_SIZE(add_on_installation_sub_product.add_on_crm_account_names) = 0, NULL, add_on_installation_sub_product.add_on_crm_account_names)
      ),
      ' ,'
    )                                                                                                                                        AS enabled_by_crm_account_name_at_event_time,
    ARRAY_TO_STRING(
      COALESCE(
        IFF(ARRAY_SIZE(namespace_subscription.enabled_by_account_owners) = 0, NULL, namespace_subscription.enabled_by_account_owners),
        IFF(ARRAY_SIZE(installation_subscription.account_owners) = 0, NULL, installation_subscription.account_owners)
      ),
      ' ,'
    )                                                                                                                                        AS enabled_by_account_owner_at_event_time,
    ARRAY_TO_STRING(
      COALESCE(
        IFF(ARRAY_SIZE(namespace_subscription.enabled_by_parent_crm_account_geos) = 0, NULL, namespace_subscription.enabled_by_parent_crm_account_geos),
        IFF(ARRAY_SIZE(installation_subscription.parent_crm_account_geos) = 0, NULL, installation_subscription.parent_crm_account_geos)
      ),
      ' ,'
    )                                                                                                                                        AS enabled_by_parent_crm_account_geo_at_event_time,
    ARRAY_TO_STRING(
      COALESCE(
        IFF(ARRAY_SIZE(namespace_subscription.enabled_by_parent_crm_account_sales_segments) = 0, NULL, namespace_subscription.enabled_by_parent_crm_account_sales_segments),
        IFF(ARRAY_SIZE(installation_subscription.parent_crm_account_sales_segments) = 0, NULL, installation_subscription.parent_crm_account_sales_segments)
      ),
      ' ,'
    )                                                                                                                                        AS enabled_by_parent_crm_account_sales_segment_at_event_time,
    ARRAY_TO_STRING(
      COALESCE(
        IFF(ARRAY_SIZE(namespace_subscription.enabled_by_parent_crm_account_industries) = 0, NULL, namespace_subscription.enabled_by_parent_crm_account_industries),
        IFF(ARRAY_SIZE(installation_subscription.parent_crm_account_industries) = 0, NULL, installation_subscription.parent_crm_account_industries)
      ),
      ' ,'
    )                                                                                                                                        AS enabled_by_parent_crm_account_industry_at_event_time,
    ARRAY_TO_STRING(
      COALESCE(
        IFF(ARRAY_SIZE(namespace_subscription.enabled_by_technical_account_managers) = 0, NULL, namespace_subscription.enabled_by_technical_account_managers),
        IFF(ARRAY_SIZE(installation_subscription.technical_account_managers) = 0, NULL, installation_subscription.technical_account_managers)
      ),
      ' ,'
    )                                                                                                                                        AS enabled_by_technical_account_manager_at_event_time,
    ARRAY_TO_STRING(
      COALESCE(
        IFF(ARRAY_SIZE(namespace_subscription.enabled_by_parent_crm_account_regions) = 0, NULL, namespace_subscription.enabled_by_parent_crm_account_regions),
        IFF(ARRAY_SIZE(installation_subscription.parent_crm_account_regions) = 0, NULL, installation_subscription.parent_crm_account_regions)
      ),
      ' ,'
    )                                                                                                                                        AS enabled_by_parent_crm_account_region_at_event_time,
    COALESCE(
      IFF(ARRAY_SIZE(namespace_subscription.enabled_by_product_tier_names) = 0, NULL, namespace_subscription.enabled_by_product_tier_names),
      IFF(ARRAY_SIZE(installation_subscription.product_tier_names) = 0, NULL, installation_subscription.product_tier_names)
    )                                                                                                                                        AS enabled_by_product_tier_names_at_event_time,
    COALESCE(
      IFF(ARRAY_SIZE(namespace_subscription.enabled_by_product_categories) = 0, NULL, namespace_subscription.enabled_by_product_categories),
      IFF(ARRAY_SIZE(installation_subscription.product_categories) = 0, NULL, installation_subscription.product_categories)
    )                                                                                                                                        AS enabled_by_product_at_event_time,
    ARRAY_TO_STRING(
      COALESCE(
        IFF(ARRAY_SIZE(add_on_namespace_sub_product.enabled_by_add_on_dim_subscription_ids) = 0, NULL, add_on_namespace_sub_product.enabled_by_add_on_dim_subscription_ids),
        IFF(ARRAY_SIZE(add_on_installation_sub_product.add_on_dim_subscription_ids) = 0, NULL, add_on_installation_sub_product.add_on_dim_subscription_ids)
      ),
      ' ,'
    )                                                                                                                                        AS enabled_by_add_on_dim_subscription_id_at_event_time,
    ARRAY_TO_STRING(
      COALESCE(
        IFF(ARRAY_SIZE(add_on_namespace_sub_product.enabled_by_add_on_product_categories) = 0, NULL, add_on_namespace_sub_product.enabled_by_add_on_product_categories),
        IFF(ARRAY_SIZE(add_on_installation_sub_product.add_on_product_categories) = 0, NULL, add_on_installation_sub_product.add_on_product_categories)
      ),
      ' ,'
    )                                                                                                                                        AS enabled_by_add_on_product_at_event_time,
    ARRAY_TO_STRING(
      COALESCE(
        IFF(ARRAY_SIZE(add_on_sm_trial.add_on_trial_product_categories) = 0, NULL, add_on_sm_trial.add_on_trial_product_categories),
        IFF(ARRAY_SIZE(TO_ARRAY(dotcom_duo_trials.product_rate_plan_category_general)) = 0, NULL, TO_ARRAY(dotcom_duo_trials.product_rate_plan_category_general))
      ),
      ' ,'
    )                                                                                                                                        AS enabled_by_add_on_trial_product_at_event_time,
    COALESCE(
      namespace_subscription.enabled_by_oss_or_edu_rate_plan,
      installation_subscription.oss_or_edu_rate_plans
    )                                                                                                                                        AS enabled_by_oss_or_edu_rate_plan_at_event_time,
    COALESCE(
      paid_duo_namespace.has_paid_duo_namespace,
      paid_duo_installation.has_paid_duo_installation,
      FALSE
    )                                                                                                                                        AS is_paid_duo,
    CASE
      WHEN enabled_by_product_deployment_type = 'GitLab.com' THEN enabled_by_ultimate_parent_namespace_id::VARCHAR
      WHEN flattened_with_installation_id.dim_installation_id IS NOT NULL THEN flattened_with_installation_id.dim_installation_id
    END                                                                                                                                      AS enabled_by_product_entity_id,
    CASE
      WHEN enabled_by_product_deployment_type = 'GitLab.com' THEN 'ultimate_parent_namespace_id'
      WHEN flattened_with_installation_id.dim_installation_id IS NOT NULL THEN 'flattened_with_installation_id.dim_installation_id'
    END                                                                                                                                      AS enabled_by_product_entity_type,
    CASE
      WHEN enabled_by_product_deployment_type = 'GitLab.com' THEN enabled_by_internal_namespace
      WHEN flattened_with_installation_id.dim_installation_id IS NOT NULL THEN enabled_by_internal_installation
      ELSE FALSE
    END                                                                                                                                      AS enabled_by_internal_product_entity,
    CASE
      WHEN enabled_by_product_tier_names_at_event_time IS NULL THEN 'No Product Tier Subscription'
      ELSE ARRAY_TO_STRING(enabled_by_product_tier_names_at_event_time, ', ')
    END                                                                                                                                      AS enabled_by_product_tier,
    CASE
      WHEN enabled_by_add_on_product_at_event_time LIKE '%Duo Enterprise%' THEN 'Duo Enterprise Subscription'
      WHEN enabled_by_add_on_product_at_event_time LIKE '%Duo Pro%' THEN 'Duo Pro Subscription'
      WHEN enabled_by_add_on_product_at_event_time LIKE '%Duo with Amazon Q%' THEN 'Duo with Amazon Q Subscription'
      END                                                                                                                                    AS duo_subscriptions_clean,
    CASE
       WHEN enabled_by_add_on_trial_product_at_event_time LIKE '%Duo Enterprise%' THEN 'Duo Enterprise Trial'
       WHEN enabled_by_add_on_trial_product_at_event_time LIKE '%Duo Pro%' THEN 'Duo Pro Trial'
       WHEN enabled_by_add_on_trial_product_at_event_time LIKE '%Duo with Amazon Q%' THEN 'Duo with Amazon Q Trial'
       END                                                                                                                                   AS duo_trial_clean,
    CASE
      WHEN feature_enablement_type = 'duo_core' THEN 'Duo Core' 
      WHEN duo_subscriptions_clean IS NOT NULL AND duo_trial_clean IS NOT NULL THEN duo_subscriptions_clean || ', ' || duo_trial_clean
      WHEN duo_subscriptions_clean IS NOT NULL THEN duo_subscriptions_clean
      WHEN duo_trial_clean IS NOT NULL THEN duo_trial_clean
      ELSE 'None'
    END                                                                                                                                     AS enabled_by_duo_add_on_detail,
    CASE
      WHEN feature_enablement_type = 'duo_core' THEN 'Duo Core' 
      WHEN duo_subscriptions_clean IS NOT NULL THEN 'Duo Subscription'
      WHEN duo_trial_clean IS NOT NULL THEN 'Duo Trial'
      ELSE 'None'
    END                                                                                                                                      AS enabled_by_duo_add_on, 
    -- returning usage prep calculations below
    -- Using standard methodology to identify internal usage
    CASE WHEN (enabled_by_internal_product_entity = TRUE OR gsc_is_gitlab_team_member = TRUE) AND duo_subscriptions_clean IS NULL THEN TRUE -- excluding rare edgecases where 'internal' usage is enabled by a customer with a subscription
      WHEN enabled_by_internal_product_entity = FALSE OR gsc_is_gitlab_team_member = FALSE OR duo_subscriptions_clean IS NOT NULL THEN FALSE END 
      AS is_internal_usage_any,
    CASE
      WHEN feature_enablement_type = 'duo_core' THEN 'Duo Core' 
      WHEN feature_enablement_type = 'duo_pro' THEN 'Duo Pro'
      WHEN feature_enablement_type = 'duo_enterprise' THEN 'Duo Enterprise'
      WHEN feature_enablement_type = 'duo_with_amazon_q' THEN 'Duo with Amazon Q'
      WHEN duo_subscriptions_clean IS NOT NULL THEN REPLACE(duo_subscriptions_clean, ' Subscription', '')
      WHEN duo_trial_clean IS NOT NULL THEN REPLACE(duo_trial_clean, ' Trial', '')
      ELSE 'None'
    END                                                                                                                                     AS enabled_by_duo_category --enablement does not indicate the presence of a subscription or trial by default
  FROM flattened_with_installation_id
  LEFT JOIN dim_namespace
    ON flattened_with_installation_id.enabled_by_namespace_id = dim_namespace.dim_namespace_id
      AND flattened_with_installation_id.deployment_type = 'GitLab.com'
  LEFT JOIN dim_app_release_major_minor
    ON flattened_with_installation_id.major_minor_version = dim_app_release_major_minor.major_minor_version
  LEFT JOIN namespace_subscription
    ON dim_namespace.dim_namespace_id = namespace_subscription.dim_namespace_id
      AND flattened_with_installation_id.behavior_date = namespace_subscription.date_actual
  LEFT JOIN add_on_namespace_sub_product
    ON dim_namespace.dim_namespace_id = add_on_namespace_sub_product.dim_namespace_id
      AND flattened_with_installation_id.behavior_date = add_on_namespace_sub_product.date_actual
  LEFT JOIN installation_subscription
    ON flattened_with_installation_id.dim_installation_id = installation_subscription.dim_installation_id
      AND flattened_with_installation_id.behavior_date = installation_subscription.date_actual
  LEFT JOIN add_on_installation_sub_product
    ON flattened_with_installation_id.dim_installation_id = add_on_installation_sub_product.dim_installation_id
      AND flattened_with_installation_id.behavior_date = add_on_installation_sub_product.date_actual
  LEFT JOIN dotcom_duo_trials
    ON dim_namespace.dim_namespace_id = dotcom_duo_trials.dim_namespace_id
      AND flattened_with_installation_id.behavior_at >= dotcom_duo_trials.earliest_trial_start_date
      AND flattened_with_installation_id.behavior_at <= dotcom_duo_trials.latest_trial_end_date
  LEFT JOIN add_on_sm_trial
    ON flattened_with_installation_id.dim_installation_id = add_on_sm_trial.dim_installation_id
      AND flattened_with_installation_id.behavior_date = add_on_sm_trial.date_actual
  LEFT JOIN paid_duo_namespace
    ON dim_namespace.dim_namespace_id = paid_duo_namespace.dim_namespace_id
      AND flattened_with_installation_id.behavior_date = paid_duo_namespace.date_actual
  LEFT JOIN paid_duo_installation
    ON flattened_with_installation_id.dim_installation_id = paid_duo_installation.dim_installation_id
      AND flattened_with_installation_id.behavior_date = paid_duo_installation.date_actual

)

SELECT *
FROM joined
