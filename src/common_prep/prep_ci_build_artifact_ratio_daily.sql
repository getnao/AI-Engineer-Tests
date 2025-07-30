WITH final AS (

  SELECT
    prep_gitlab_dotcom_project_statistics_daily_snapshot.snapshot_day,
    prep_gitlab_dotcom_plan.plan_name_modified                                                     AS plan_name,
    COALESCE(prep_gitlab_dotcom_project_statistics_daily_snapshot.finance_pl_category, 'internal') AS finance_pl_category,
    SUM(prep_gitlab_dotcom_project_statistics_daily_snapshot.build_artifacts_gb)                   AS build_artifacts_gb,
    RATIO_TO_REPORT(SUM(prep_gitlab_dotcom_project_statistics_daily_snapshot.build_artifacts_gb))
      OVER (PARTITION BY prep_gitlab_dotcom_project_statistics_daily_snapshot.snapshot_day)        AS percent_build_artifacts_size
  FROM {{ ref('prep_gitlab_dotcom_project_statistics_daily_snapshot') }}
  LEFT JOIN {{ ref('prep_gitlab_dotcom_plan') }}
    ON prep_gitlab_dotcom_project_statistics_daily_snapshot.ultimate_parent_plan_id = prep_gitlab_dotcom_plan.dim_plan_id
  GROUP BY
    1, 2, 3

),

build_artifacts_pl_daily AS (

  SELECT
    snapshot_day                 AS date_day,
    'gitlab-production'          AS gcp_project_id,
    'Cloud Storage'              AS gcp_service_description,
    NULL                         AS gcp_sku_description,
    plan_name                    AS plan_name,
    'build_artifacts'            AS infra_label,
    NULL                         AS env_label,
    NULL                         AS runner_label,
    NULL                         AS full_path,
    LOWER(finance_pl_category)   AS pl_category,
    percent_build_artifacts_size AS pl_percent,
    'build_artifacts_pl_daily'   AS from_mapping
  FROM final

),

build_artifacts_pl_dev_daily AS (

  SELECT DISTINCT
    snapshot_day                   AS date_day,
    'gitlab-production'            AS gcp_project_id,
    'Cloud Storage'                AS gcp_service_description,
    NULL                           AS gcp_sku_description,
    NULL                           AS plan_name,
    'build_artifacts'              AS infra_label,
    'dev'                          AS env_label,
    NULL                           AS runner_label,
    NULL                           AS full_path,
    'internal'                     AS pl_category,
    1                              AS pl_percent,
    'build_artifacts_pl_dev_daily' AS from_mapping
  FROM final

)

SELECT *
FROM build_artifacts_pl_daily
UNION ALL
SELECT *
FROM build_artifacts_pl_dev_daily
