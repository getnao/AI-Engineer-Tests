WITH final AS (

  SELECT
    prep_gitlab_dotcom_project_statistics_daily_snapshot.snapshot_day,
    prep_gitlab_dotcom_plan.plan_name_modified                                                     AS plan_name,
    COALESCE(prep_gitlab_dotcom_project_statistics_daily_snapshot.finance_pl_category, 'internal') AS finance_pl_category,
    SUM(prep_gitlab_dotcom_project_statistics_daily_snapshot.container_registry_gb)                AS container_registry_gb,
    RATIO_TO_REPORT(SUM(prep_gitlab_dotcom_project_statistics_daily_snapshot.container_registry_gb))
      OVER (PARTITION BY prep_gitlab_dotcom_project_statistics_daily_snapshot.snapshot_day)        AS percent_container_registry_size
  FROM {{ ref('prep_gitlab_dotcom_project_statistics_daily_snapshot') }}
  LEFT JOIN {{ ref('prep_gitlab_dotcom_plan') }}
    ON prep_gitlab_dotcom_project_statistics_daily_snapshot.ultimate_parent_plan_id = prep_gitlab_dotcom_plan.dim_plan_id
  GROUP BY
    1, 2, 3

),

container_registry_pl_daily AS (

  SELECT
    snapshot_day                       AS date_day,
    'gitlab-production'                AS gcp_project_id,
    'Cloud Storage'                    AS gcp_service_description,
    'Standard Storage US Multi-region' AS gcp_sku_description,
    plan_name                          AS plan_name,
    'registry'                         AS infra_label,
    NULL                               AS env_label,
    NULL                               AS runner_label,
    NULL                               AS full_path,
    LOWER(finance_pl_category)         AS pl_category,
    percent_container_registry_size    AS pl_percent,
    'container_registry_pl_daily'      AS from_mapping
  FROM final
  WHERE snapshot_day > '2022-06-10'

),

container_registry_pl_daily_ext AS (

  SELECT
    snapshot_day                    AS date_day,
    'gitlab-production'             AS gcp_project_id,
    NULL                            AS gcp_service_description,
    NULL                            AS gcp_sku_description,
    plan_name                       AS plan_name,
    'registry'                      AS infra_label,
    NULL                            AS env_label,
    NULL                            AS runner_label,
    NULL                            AS full_path,
    LOWER(finance_pl_category)      AS pl_category,
    percent_container_registry_size AS pl_percent,
    'container_registry_pl_daily'   AS from_mapping
  FROM final
  WHERE snapshot_day > '2022-06-10'

)

SELECT *
FROM container_registry_pl_daily
UNION ALL
SELECT *
FROM container_registry_pl_daily_ext
