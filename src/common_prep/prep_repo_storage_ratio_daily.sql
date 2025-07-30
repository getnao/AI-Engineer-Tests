WITH final AS (

  SELECT
    prep_gitlab_dotcom_project_statistics_daily_snapshot.snapshot_day,
    prep_gitlab_dotcom_plan.plan_name_modified                                                     AS plan_name,
    COALESCE(prep_gitlab_dotcom_project_statistics_daily_snapshot.finance_pl_category, 'internal') AS finance_pl_category,
    SUM(prep_gitlab_dotcom_project_statistics_daily_snapshot.repo_size_gb)                         AS repo_size_gb,
    RATIO_TO_REPORT(SUM(prep_gitlab_dotcom_project_statistics_daily_snapshot.repo_size_gb))
      OVER (PARTITION BY prep_gitlab_dotcom_project_statistics_daily_snapshot.snapshot_day)        AS percent_repo_size_gb
  FROM {{ ref('prep_gitlab_dotcom_project_statistics_daily_snapshot') }}
  LEFT JOIN {{ ref('prep_gitlab_dotcom_plan')}}
    ON prep_gitlab_dotcom_project_statistics_daily_snapshot.ultimate_parent_plan_id = prep_gitlab_dotcom_plan.dim_plan_id
  GROUP BY
    1, 2, 3

),

repo_storage_pl_daily AS ( -- gitaly costs in production project

  SELECT
    snapshot_day               AS date_day,
    'gitlab-production'        AS gcp_project_id,
    NULL                       AS gcp_service_description,
    NULL                       AS gcp_sku_description,
    plan_name                  AS plan_name,
    'gitaly'                   AS infra_label,
    NULL                       AS env_label,
    NULL                       AS runner_label,
    NULL                       AS full_path,
    LOWER(finance_pl_category) AS pl_category,
    percent_repo_size_gb       AS pl_percent,
    'repo_storage_pl_daily'    AS from_mapping
  FROM final

),

repo_storage_pl_daily_ext AS ( -- gitaly costs in gitlab-gitaly-gprd-* projects

  SELECT
    snapshot_day               AS date_day,
    'gitlab-gitaly-gprd-%'     AS gcp_project_id,
    NULL                       AS gcp_service_description,
    NULL                       AS gcp_sku_description,
    plan_name                  AS plan_name,
    'gitaly'                   AS infra_label,
    NULL                       AS env_label,
    NULL                       AS runner_label,
    NULL                       AS full_path,
    LOWER(finance_pl_category) AS pl_category,
    percent_repo_size_gb       AS pl_percent,
    'repo_storage_pl_daily'    AS from_mapping
  FROM final

),

repo_storage_pl_daily_cdn AS ( --apply gitaly repository storage split to cdn skus

  SELECT
    snapshot_day               AS date_day,
    'gitlab-production'        AS gcp_project_id,
    'Cloud Storage'            AS gcp_service_description,
    sku_list.sku               AS gcp_sku_description,
    plan_name                  AS plan_name,
    NULL                       AS infra_label,
    NULL                       AS env_label,
    NULL                       AS runner_label,
    NULL                       AS full_path,
    LOWER(finance_pl_category) AS pl_category,
    percent_repo_size_gb       AS pl_percent,
    'repo_storage_pl_daily'    AS from_mapping
  FROM final
  CROSS JOIN (
    SELECT 'Cloud CDN Cache Fill from North America to Europe' AS sku
    UNION ALL
    SELECT 'Cloud CDN North America Intra-Region Cache Fill'
    UNION ALL
    SELECT 'Cloud CDN Cache Fill from North America to Asia Pacific'
    UNION ALL
    SELECT 'Cloud CDN Cache Fill from North America to Oceania'
  ) AS sku_list

)

SELECT *
FROM repo_storage_pl_daily
UNION ALL
SELECT *
FROM repo_storage_pl_daily_ext
UNION ALL
SELECT *
FROM repo_storage_pl_daily_cdn
