WITH export AS (

  SELECT *
  FROM {{ ref('fct_gcp_billing_line_item') }}
  WHERE invoice_month >= '2023-01-01'

),

infra_labels AS (

  SELECT *
  FROM {{ ref('prep_gcp_billing_resource_label') }}
  WHERE resource_label_key = 'gl_product_category'

),

env_labels AS (

  SELECT *
  FROM {{ ref('prep_gcp_billing_resource_label') }}
  WHERE resource_label_key = 'env'

),

runner_labels AS (

  SELECT
    source_primary_key,
    CASE
      WHEN resource_label_value LIKE '%gpu%'
        THEN
          CASE
            WHEN resource_label_value LIKE '%runners-manager-saas-linux-medium-%'
              THEN '8 - shared saas runners gpu - medium'
            WHEN resource_label_value LIKE '%runners-manager-saas-linux-large-%'
              THEN '9 - shared saas runners gpu - large'
            ELSE resource_label_value
          END
      WHEN resource_label_value LIKE '%runners-manager-shared-blue-%'
        THEN '2 - shared saas runners - small' --ok
      WHEN resource_label_value LIKE '%runners-manager-shared-green-%'
        THEN '2 - shared saas runners - small' --ok
      WHEN resource_label_value LIKE '%gitlab-shared-runners-manager-%'
        THEN '1 - shared gitlab org runners' --ok
      WHEN resource_label_value LIKE '%shared-runners-manager-%'
        THEN '2 - shared saas runners - small' --ok
      WHEN resource_label_value LIKE '%runners-manager-saas-linux-small-amd64-%'
        THEN '2 - shared saas runners - small'
      WHEN resource_label_value LIKE '%runners-manager-saas-linux-medium-amd64-%'
        THEN '3 - shared saas runners - medium' --ok
      WHEN resource_label_value LIKE '%runners-manager-saas-linux-large-amd64-%'
        THEN '4 - shared saas runners - large' --ok
      WHEN resource_label_value LIKE '%runners-manager-saas-linux-xlarge-amd64-%'
        THEN '10 - shared saas runners - xlarge'
      WHEN resource_label_value LIKE '%runners-manager-saas-linux-2xlarge-amd64-%'
        THEN '11 - shared saas runners - 2xlarge'
      WHEN resource_label_value LIKE '%runners-manager-saas-macos-staging-%'
        THEN 'runners-manager-saas-macos-staging-'
      WHEN resource_label_value LIKE '%runners-manager-saas-macos%-m1-%'
        THEN '5 - shared saas macos runners'
      WHEN resource_label_value LIKE '%runners-manager-shared-gitlab-org-%'
        THEN '1 - shared gitlab org runners'
      WHEN resource_label_value LIKE '%runners-manager-private-%'
        THEN '6 - private internal runners'
      WHEN resource_label_value LIKE '%private-runners-manager-%'
        THEN '6 - private internal runners'
      WHEN (resource_label_value LIKE '%instances/runner-%' AND resource_label_value LIKE '%shared-gitlab-org-%')
        THEN '1 - shared gitlab org runners'
      WHEN (resource_label_value LIKE '%instances/runner-%' AND resource_label_value LIKE '%amd64%')
        THEN 'runners-saas'
      WHEN (resource_label_value LIKE '%instances/runner-%' AND resource_label_value LIKE '%s-shared-%')
        THEN '2 - shared saas runners - small'
      WHEN (resource_label_value LIKE '%instances/runner-%' AND resource_label_value LIKE '%-shared-%' AND resource_label_value NOT LIKE '%gitlab%')
        THEN '2 - shared saas runners - small'
      WHEN (resource_label_value LIKE '%instances/runner-%' AND resource_label_value LIKE '%-private-%')
        THEN '6 - private internal runners'
      WHEN resource_label_value LIKE '%gke-runners-gke-default-pool-%'
        THEN 'gke-runners-gke-default-pool-'
      WHEN resource_label_value LIKE '%test-machine-%'
        THEN 'test-machine-'
      WHEN resource_label_value LIKE '%tm-runner-%'
        THEN 'tm-runner-'
      WHEN resource_label_value LIKE '%tm-test-instance%'
        THEN 'tm-test-instance'
      WHEN resource_label_value LIKE '%gitlab-temporary-gcp-image-%'
        THEN 'gitlab-temporary-gcp-image-'
      WHEN resource_label_value LIKE '%sd-exporter%'
        THEN 'sd-exporter'
      WHEN resource_label_value LIKE '%/bastion-%'
        THEN 'bastion'
      WHEN resource_label_value LIKE '%/gitlab-qa-tunnel%'
        THEN 'gitlab-qa-tunnel'
      ELSE resource_label_value
    END AS resource_label_value
  FROM {{ ref('prep_gcp_billing_resource_label') }}
  WHERE resource_label_key = 'runner_manager_name'

),

full_path AS (

  SELECT *
  FROM {{ ref('prep_gcp_billing_project_full_path') }}

),

billing_base AS (

  SELECT
    DATE(export.usage_start_time)                                       AS day,
    COALESCE(export.gcp_project_id, 'no_id')                            AS gcp_project_id,
    export.gcp_service_description,
    export.gcp_sku_description,
    infra_labels.resource_label_value                                   AS infra_label,
    env_labels.resource_label_value                                     AS env_label,
    runner_labels.resource_label_value                                  AS runner_label,
    export.usage_unit,
    export.pricing_unit,
    full_path.full_path,
    SUM(export.usage_amount)                                            AS usage_amount,
    SUM(export.usage_amount_in_pricing_units)                           AS usage_amount_in_pricing_units,
    SUM(export.cost_before_credits)                                     AS cost_before_credits,
    SUM(export.cost_before_credits + COALESCE(export.total_credits, 0)) AS net_cost
  FROM export
  LEFT JOIN infra_labels
    ON export.gcp_billing_line_item_pk = infra_labels.source_primary_key
  LEFT JOIN env_labels
    ON export.gcp_billing_line_item_pk = env_labels.source_primary_key
  LEFT JOIN runner_labels
    ON export.gcp_billing_line_item_pk = runner_labels.source_primary_key
  LEFT JOIN full_path
    ON (
      export.gcp_project_id = full_path.gcp_project_id
      AND DATE_TRUNC('day', export.usage_start_time) >= DATE_TRUNC('day', full_path.first_created_at)
      AND DATE_TRUNC('day', export.usage_start_time) <= DATEADD('day', -1, DATE_TRUNC('day', full_path.last_updated_at))
    )
  {{ dbt_utils.group_by(n=10) }}

),

combined_pl_mapping AS (

  SELECT *
  FROM {{ ref('prep_gcp_billing_attribute_ratio_daily') }}
  UNION ALL
  SELECT *
  FROM {{ ref('prep_repo_storage_ratio_daily') }}
  UNION ALL
  SELECT *
  FROM {{ ref('prep_container_registry_ratio_daily') }}
  UNION ALL
  SELECT *
  FROM {{ ref('prep_ci_build_artifact_ratio_daily') }}
  UNION ALL
  SELECT *
  FROM {{ ref('prep_ci_runner_ratio_daily') }}
  UNION ALL
  SELECT *
  FROM {{ ref('prep_haproxy_ratio_daily') }}
  UNION ALL
  SELECT *
  FROM {{ ref('prep_pubsub_ratio_daily') }}

),

split_by_pl_pct AS (

  SELECT
    billing_base.day                                                                         AS date_day,
    billing_base.gcp_project_id,
    billing_base.gcp_service_description,
    billing_base.gcp_sku_description,
    billing_base.infra_label,
    billing_base.env_label,
    billing_base.runner_label,
    billing_base.full_path,
    combined_pl_mapping.plan_name,
    combined_pl_mapping.pl_category,
    billing_base.usage_unit,
    billing_base.pricing_unit,
    billing_base.usage_amount * COALESCE(combined_pl_mapping.pl_percent, 1)                  AS usage_amount,
    billing_base.usage_amount_in_pricing_units * COALESCE(combined_pl_mapping.pl_percent, 1) AS usage_amount_in_pricing_units,
    billing_base.cost_before_credits * COALESCE(combined_pl_mapping.pl_percent, 1)           AS cost_before_credits,
    billing_base.net_cost * COALESCE(combined_pl_mapping.pl_percent, 1)                      AS net_cost,
    combined_pl_mapping.from_mapping,
    DENSE_RANK() OVER (
      PARTITION BY
        billing_base.day,
        billing_base.gcp_project_id,
        billing_base.gcp_service_description,
        billing_base.gcp_sku_description,
        billing_base.infra_label,
        billing_base.env_label,
        billing_base.runner_label,
        billing_base.full_path
      ORDER BY
        (CASE WHEN combined_pl_mapping.full_path IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN combined_pl_mapping.gcp_service_description IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN combined_pl_mapping.gcp_sku_description IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN combined_pl_mapping.infra_label IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN combined_pl_mapping.env_label IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN combined_pl_mapping.runner_label IS NOT NULL THEN 1 ELSE 0 END) DESC,
        (CASE WHEN combined_pl_mapping.gcp_project_id IS NOT NULL THEN 1 ELSE 0 END) DESC
    )                                                                                        AS priority
  FROM billing_base
  LEFT JOIN combined_pl_mapping
    ON
      -- Match on date (required)
      billing_base.day = combined_pl_mapping.date_day

      -- Match on project ID (if specified in mapping)
      AND (
        combined_pl_mapping.gcp_project_id IS NULL
        OR billing_base.gcp_project_id LIKE combined_pl_mapping.gcp_project_id
      )

      -- Match on service description (if specified in mapping)
      AND (
        combined_pl_mapping.gcp_service_description IS NULL
        OR billing_base.gcp_service_description = combined_pl_mapping.gcp_service_description
      )

      -- Match on SKU description (if specified in mapping)
      AND (
        combined_pl_mapping.gcp_sku_description IS NULL
        OR billing_base.gcp_sku_description = combined_pl_mapping.gcp_sku_description
      )

      -- Match on infrastructure label (if specified in mapping)
      AND (
        combined_pl_mapping.infra_label IS NULL
        OR COALESCE(billing_base.infra_label, '') = combined_pl_mapping.infra_label
      )

      -- Match on environment label (if specified in mapping)
      AND (
        combined_pl_mapping.env_label IS NULL
        OR COALESCE(billing_base.env_label, '') = combined_pl_mapping.env_label
      )

      -- Match on runner label (if specified in mapping)
      AND (
        combined_pl_mapping.runner_label IS NULL
        OR COALESCE(billing_base.runner_label, '') = combined_pl_mapping.runner_label
      )

      -- Match on full path (with wildcard support if specified in mapping)
      AND (
        combined_pl_mapping.full_path IS NULL
        OR COALESCE(billing_base.full_path, '') LIKE combined_pl_mapping.full_path
      )
)

SELECT
  * EXCLUDE (priority),
  {{ dbt_utils.generate_surrogate_key([ 'date_day', 'gcp_project_id', 'gcp_service_description', 'gcp_sku_description', 'infra_label', 'env_label', 'runner_label', 'full_path', 'plan_name', 'pl_category', 'from_mapping']) }} AS gcp_billing_pl_allocation_pk
FROM split_by_pl_pct
WHERE priority = 1
