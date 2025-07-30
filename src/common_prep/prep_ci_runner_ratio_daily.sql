WITH ci_minutes AS (

  SELECT
    DATE_TRUNC('day', ci_build_started_at)::DATE  AS reporting_day,
    prep_gitlab_dotcom_plan.plan_name_modified    AS plan_name,
    CASE
      WHEN plan_name_modified LIKE '%trial%' THEN 'free'
      WHEN (plan_name_modified = 'ultimate' AND dim_namespace.namespace_is_internal = FALSE) THEN 'paid'
      WHEN (plan_name_modified = 'ultimate' AND dim_namespace.namespace_is_internal = TRUE) THEN 'internal'
      WHEN plan_name_modified = 'premium' THEN 'paid'
      WHEN plan_name_modified = 'default' THEN 'free'
      WHEN plan_title = 'Bronze' THEN 'paid'
      WHEN plan_title = 'Open Source Program' THEN 'free'
      WHEN plan_title = 'Free' THEN 'free'
      ELSE plan_title
    END                                           AS pl,
    CASE
      WHEN ci_runner_type_summary = 'shared' THEN 'Shared Runners'
      ELSE 'Self-Managed Runners'
    END                                           AS runner_type,

    CASE
      WHEN LOWER(ci_runner_description) LIKE '%-_.saas-linux-medium-amd64-gpu%' THEN '%-_.saas-linux-medium-amd64-gpu'
      WHEN LOWER(ci_runner_description) LIKE '%-_.saas-linux-large-amd64-gpu%' THEN '%-_.saas-linux-large-amd64-gpu'
      WHEN LOWER(ci_runner_description) LIKE '%-_.saas-linux-medium-amd64%' THEN '%-_.saas-linux-medium-amd64'
      WHEN LOWER(ci_runner_description) LIKE '%-_.saas-linux-large-amd64%' THEN '%-_.saas-linux-large-amd64'
      WHEN LOWER(ci_runner_description) LIKE '%.saas-linux-small-amd64%' THEN '%.saas-linux-small-amd64'
      WHEN LOWER(ci_runner_description) LIKE '%.saas-linux-xlarge-amd64%' THEN '%.saas-linux-xlarge-amd64'
      WHEN LOWER(ci_runner_description) LIKE '%.saas-linux-2xlarge-amd64%' THEN '%.saas-linux-2xlarge-amd64'
      WHEN LOWER(ci_runner_description) LIKE 'macos shared%' OR LOWER(ci_runner_description) LIKE '%.saas-macos-medium-m1.runners-manager%' THEN 'macos shared runners'
      ELSE ci_runner_manager
    END                                          AS ci_runner_manager,

    is_paid_by_gitlab,

    SUM(ci_build_duration_in_s) / 60             AS ci_build_minutes

  FROM {{ ref('fct_ci_runner_activity') }} --common.fct_ci_runner_activity 
  INNER JOIN {{ ref('dim_ci_runner') }} --common.dim_ci_runner
    ON fct_ci_runner_activity.dim_ci_runner_id = dim_ci_runner.dim_ci_runner_id
  INNER JOIN {{ ref('dim_namespace') }} --common.dim_namespace
    ON fct_ci_runner_activity.dim_namespace_id = dim_namespace.dim_namespace_id
  INNER JOIN {{ ref('prep_gitlab_dotcom_plan') }} --common_prep.prep_gitlab_dotcom_plan
    ON fct_ci_runner_activity.dim_plan_id = prep_gitlab_dotcom_plan.dim_plan_id
  WHERE DATE_TRUNC('month', ci_build_started_at) >= '2023-02-01' -- FY23 start date for data recency and accuracy purposes
    AND ci_build_finished_at IS NOT NULL
    AND namespace_creator_is_blocked = FALSE
  {{ dbt_utils.group_by(n=6) }}

),

mapped AS (

  SELECT
    reporting_day,
    plan_name,
    CASE
      WHEN runner_type = 'Self-Managed Runners' AND ci_runner_manager = 'private-runner-mgr' THEN '6 - private internal runners'
      WHEN runner_type = 'Self-Managed Runners' AND is_paid_by_gitlab = TRUE THEN '6 - private internal runners'
      WHEN runner_type = 'Shared Runners' AND ci_runner_manager = 'shared-gitlab-org-runner-mgr' THEN '1 - shared gitlab org runners'
      WHEN ci_runner_manager LIKE '%gpu%'
        THEN
          CASE WHEN runner_type = 'Shared Runners' AND ci_runner_manager = '%-_.saas-linux-medium-amd64-gpu' THEN '8 - shared saas runners gpu - medium'
            WHEN runner_type = 'Shared Runners' AND ci_runner_manager = '%-_.saas-linux-large-amd64-gpu' THEN '9 - shared saas runners gpu - large'
          END
      WHEN runner_type = 'Shared Runners' AND ci_runner_manager = '%.saas-linux-small-amd64' THEN '2 - shared saas runners - small'
      WHEN runner_type = 'Shared Runners' AND ci_runner_manager = '%-_.saas-linux-medium-amd64' THEN '3 - shared saas runners - medium'
      WHEN runner_type = 'Shared Runners' AND ci_runner_manager = '%-_.saas-linux-large-amd64' THEN '4 - shared saas runners - large'
      WHEN runner_type = 'Shared Runners' AND ci_runner_manager = '%.saas-linux-xlarge-amd64' THEN '10 - shared saas runners - xlarge'
      WHEN runner_type = 'Shared Runners' AND ci_runner_manager = '%.saas-linux-2xlarge-amd64' THEN '11 - shared saas runners - 2xlarge'
      WHEN runner_type = 'Shared Runners' AND ci_runner_manager = 'macos shared runners' THEN '5 - shared saas macos runners'
      WHEN runner_type = 'Shared Runners' AND ci_runner_manager = 'windows-runner-mgr' THEN '7 - shared saas windows runners'
    END                   AS mapping,
    pl,
    SUM(ci_build_minutes) AS ci_build_minutes
  FROM ci_minutes
  WHERE mapping IS NOT NULL
  GROUP BY 1, 2, 3, 4
  ORDER BY 1, mapping DESC

),

final AS (

  SELECT
    reporting_day,
    plan_name,
    mapping,
    pl                                                                           AS finance_pl_category,
    CASE
      WHEN mapping = '1 - shared gitlab org runners' THEN SUM(ci_build_minutes)
      WHEN mapping = '2 - shared saas runners - small' THEN SUM(ci_build_minutes)
      WHEN mapping = '3 - shared saas runners - medium' THEN SUM(ci_build_minutes) * 2
      WHEN mapping = '4 - shared saas runners - large' THEN SUM(ci_build_minutes) * 3
      WHEN mapping = '5 - shared saas macos runners' THEN SUM(ci_build_minutes) * 6
      WHEN mapping = '6 - private internal runners' THEN SUM(ci_build_minutes)
      WHEN mapping = '7 - shared saas windows runners' THEN SUM(ci_build_minutes)
      WHEN mapping = '8 - shared saas runners gpu - medium' THEN SUM(ci_build_minutes) * 7
      WHEN mapping = '10 - shared saas runners - xlarge' THEN SUM(ci_build_minutes) * 6
      WHEN mapping = '11 - shared saas runners - 2xlarge' THEN SUM(ci_build_minutes) * 12
    END
      AS total_ci_minutes,
    RATIO_TO_REPORT(total_ci_minutes) OVER (PARTITION BY reporting_day, mapping) AS pct_ci_minutes
  FROM mapped
  GROUP BY 1, 2, 3, 4

),

runner_shared_gitlab_org AS (

  -- shared gitlab org runner
  SELECT DISTINCT
    reporting_day                   AS date_day,
    NULL                            AS gcp_project_id,
    NULL                            AS gcp_service_description,
    NULL                            AS gcp_sku_description,
    plan_name                       AS plan_name, 
    NULL                            AS infra_label,
    NULL                            AS env_label,
    '1 - shared gitlab org runners' AS runner_label,
    NULL                            AS full_path,
    finance_pl_category             AS pl_category,
    pct_ci_minutes                  AS pl_percent,
    'ci_runner_pl_daily - 1'        AS from_mapping
  FROM final
  WHERE mapping = '1 - shared gitlab org runners'

),

runner_saas_small AS (

  -- small saas runners with small infra label
  SELECT
    reporting_day                     AS date_day,
    NULL                              AS gcp_project_id,
    NULL                              AS gcp_service_description,
    NULL                              AS gcp_sku_description,
    plan_name                         AS plan_name, 
    NULL                              AS infra_label,
    NULL                              AS env_label,
    '2 - shared saas runners - small' AS runner_label,
    NULL                              AS full_path,
    finance_pl_category               AS pl_category,
    pct_ci_minutes                    AS pl_percent,
    'ci_runner_pl_daily - 2'          AS from_mapping
  FROM final
  WHERE mapping = '2 - shared saas runners - small'

),

runner_saas_small_ext AS (
  -- extension: applying same split to remaining resources in gitlab-ci-plan-free-* projects

  SELECT DISTINCT
    reporting_day            AS date_day,
    small_projects.gcp_project_id,
    NULL                     AS gcp_service_description,
    NULL                     AS gcp_sku_description,
    plan_name                AS plan_name, 
    NULL                     AS infra_label,
    NULL                     AS env_label,
    NULL                     AS runner_label,
    NULL                     AS full_path,
    finance_pl_category      AS pl_category,
    pct_ci_minutes           AS pl_percent,
    'ci_runner_pl_daily - 2' AS from_mapping
  FROM final
  CROSS JOIN (
    SELECT 'gitlab-ci-plan-free-%' AS gcp_project_id
    UNION ALL
    SELECT 'gitlab-r-saas-l-s-amd64-%'
  ) AS small_projects
  WHERE mapping = '2 - shared saas runners - small'

),

runner_saas_medium AS (

  SELECT
    reporting_day                      AS date_day,
    NULL                               AS gcp_project_id,
    NULL                               AS gcp_service_description,
    NULL                               AS gcp_sku_description,
    plan_name                          AS plan_name, 
    NULL                               AS infra_label,
    NULL                               AS env_label,
    '3 - shared saas runners - medium' AS runner_label,
    NULL                               AS full_path,
    finance_pl_category                AS pl_category,
    pct_ci_minutes                     AS pl_percent,
    'ci_runner_pl_daily - 3'           AS from_mapping
  FROM final
  WHERE mapping = '3 - shared saas runners - medium'

),

runner_saas_medium_ext AS (

  SELECT DISTINCT
    reporting_day            AS date_day,
    'gitlab-r-saas-l-m-%'    AS gcp_project_id,
    NULL                     AS gcp_service_description,
    NULL                     AS gcp_sku_description,
    plan_name                AS plan_name, 
    NULL                     AS infra_label,
    NULL                     AS env_label,
    NULL                     AS runner_label,
    NULL                     AS full_path,
    finance_pl_category      AS pl_category,
    pct_ci_minutes           AS pl_percent,
    'ci_runner_pl_daily - 3' AS from_mapping
  FROM final
  WHERE mapping = '3 - shared saas runners - medium'

),

runner_saas_large AS (

  SELECT
    reporting_day                     AS date_day,
    NULL                              AS gcp_project_id,
    NULL                              AS gcp_service_description,
    NULL                              AS gcp_sku_description,
    plan_name                         AS plan_name, 
    NULL                              AS infra_label,
    NULL                              AS env_label,
    '4 - shared saas runners - large' AS runner_label,
    NULL                              AS full_path,
    finance_pl_category               AS pl_category,
    pct_ci_minutes                    AS pl_percent,
    'ci_runner_pl_daily - 4'          AS from_mapping
  FROM final
  WHERE mapping = '4 - shared saas runners - large'

),

runner_saas_large_ext AS (

  SELECT DISTINCT
    reporting_day               AS date_day,
    'gitlab-r-saas-l-l-amd64-_' AS gcp_project_id,
    NULL                        AS gcp_service_description,
    NULL                        AS gcp_sku_description,
    plan_name                   AS plan_name, 
    NULL                        AS infra_label,
    NULL                        AS env_label,
    NULL                        AS runner_label,
    NULL                        AS full_path,
    finance_pl_category         AS pl_category,
    pct_ci_minutes              AS pl_percent,
    'ci_runner_pl_daily - 4'    AS from_mapping
  FROM final
  WHERE mapping = '4 - shared saas runners - large'

),

runner_saas_xlarge AS (

  SELECT
    reporting_day                       AS date_day,
    NULL                                AS gcp_project_id,
    NULL                                AS gcp_service_description,
    NULL                                AS gcp_sku_description,
    plan_name                           AS plan_name, 
    NULL                                AS infra_label,
    NULL                                AS env_label,
    '10 - shared saas runners - xlarge' AS runner_label,
    NULL                                AS full_path,
    finance_pl_category                 AS pl_category,
    pct_ci_minutes                      AS pl_percent,
    'ci_runner_pl_daily - 10'           AS from_mapping
  FROM final
  WHERE mapping = '10 - shared saas runners - xlarge'

),

runner_saas_xlarge_ext AS (

  SELECT DISTINCT
    reporting_day                AS date_day,
    'gitlab-r-saas-l-xl-amd64-_' AS gcp_project_id,
    NULL                         AS gcp_service_description,
    NULL                         AS gcp_sku_description,
    plan_name                    AS plan_name, 
    NULL                         AS infra_label,
    NULL                         AS env_label,
    NULL                         AS runner_label,
    NULL                         AS full_path,
    finance_pl_category          AS pl_category,
    pct_ci_minutes               AS pl_percent,
    'ci_runner_pl_daily - 10'    AS from_mapping
  FROM final
  WHERE mapping = '10 - shared saas runners - xlarge'

),

runner_saas_2xlarge AS (

  SELECT
    reporting_day                        AS date_day,
    NULL                                 AS gcp_project_id,
    NULL                                 AS gcp_service_description,
    NULL                                 AS gcp_sku_description,
    plan_name                            AS plan_name, 
    NULL                                 AS infra_label,
    NULL                                 AS env_label,
    '11 - shared saas runners - 2xlarge' AS runner_label,
    NULL                                 AS full_path,
    finance_pl_category                  AS pl_category,
    pct_ci_minutes                       AS pl_percent,
    'ci_runner_pl_daily - 11'            AS from_mapping
  FROM final
  WHERE mapping = '11 - shared saas runners - 2xlarge'

),

runner_saas_2xlarge_ext AS (

  SELECT DISTINCT
    reporting_day                 AS date_day,
    'gitlab-r-saas-l-2xl-amd64-_' AS gcp_project_id,
    NULL                          AS gcp_service_description,
    NULL                          AS gcp_sku_description,
    plan_name                     AS plan_name, 
    NULL                          AS infra_label,
    NULL                          AS env_label,
    NULL                          AS runner_label,
    NULL                          AS full_path,
    finance_pl_category           AS pl_category,
    pct_ci_minutes                AS pl_percent,
    'ci_runner_pl_daily - 11'     AS from_mapping
  FROM final
  WHERE mapping = '11 - shared saas runners - 2xlarge'

),

runner_saas_medium_gpu AS (

  SELECT
    reporting_day                          AS date_day,
    NULL                                   AS gcp_project_id,
    NULL                                   AS gcp_service_description,
    NULL                                   AS gcp_sku_description,
    plan_name                              AS plan_name, 
    NULL                                   AS infra_label,
    NULL                                   AS env_label,
    '8 - shared saas runners gpu - medium' AS runner_label,
    NULL                                   AS full_path,
    finance_pl_category                    AS pl_category,
    pct_ci_minutes                         AS pl_percent,
    'ci_runner_pl_daily - 8'               AS from_mapping
  FROM final
  WHERE mapping = '8 - shared saas runners gpu - medium'

),

runner_saas_medium_ext_gpu AS (

  SELECT DISTINCT
    reporting_day            AS date_day,
    '%-r-saas-l-m-%gpu%'     AS gcp_project_id,
    NULL                     AS gcp_service_description,
    NULL                     AS gcp_sku_description,
    plan_name                AS plan_name, 
    NULL                     AS infra_label,
    NULL                     AS env_label,
    NULL                     AS runner_label,
    NULL                     AS full_path,
    finance_pl_category      AS pl_category,
    pct_ci_minutes           AS pl_percent,
    'ci_runner_pl_daily - 8' AS from_mapping
  FROM final
  WHERE mapping = '8 - shared saas runners gpu - medium'

),

runner_saas_large_gpu AS (

  SELECT
    reporting_day                         AS date_day,
    NULL                                  AS gcp_project_id,
    NULL                                  AS gcp_service_description,
    NULL                                  AS gcp_sku_description,
    plan_name                             AS plan_name, 
    NULL                                  AS infra_label,
    NULL                                  AS env_label,
    '9 - shared saas runners gpu - large' AS runner_label,
    NULL                                  AS full_path,
    finance_pl_category                   AS pl_category,
    pct_ci_minutes                        AS pl_percent,
    'ci_runner_pl_daily - 9'              AS from_mapping
  FROM final
  WHERE mapping = '8 - shared saas runners gpu - medium' --to apply historic medium pl to large
    AND reporting_day <= '2023-06-22'

  UNION ALL

  SELECT
    reporting_day                         AS date_day,
    NULL                                  AS gcp_project_id,
    NULL                                  AS gcp_service_description,
    NULL                                  AS gcp_sku_description,
    plan_name                             AS plan_name, 
    NULL                                  AS infra_label,
    NULL                                  AS env_label,
    '9 - shared saas runners gpu - large' AS runner_label,
    NULL                                  AS full_path,
    finance_pl_category                   AS pl_category,
    pct_ci_minutes                        AS pl_percent,
    'ci_runner_pl_daily - 9'              AS from_mapping
  FROM final
  WHERE mapping = '9 - shared saas runners gpu - large'

),

runner_saas_large_ext_gpu AS (

  SELECT DISTINCT
    reporting_day            AS date_day,
    '%-r-saas-l-l-%gpu%'     AS gcp_project_id,
    NULL                     AS gcp_service_description,
    NULL                     AS gcp_sku_description,
    plan_name                AS plan_name, 
    NULL                     AS infra_label,
    NULL                     AS env_label,
    NULL                     AS runner_label,
    NULL                     AS full_path,
    finance_pl_category      AS pl_category,
    pct_ci_minutes           AS pl_percent,
    'ci_runner_pl_daily - 9' AS from_mapping
  FROM final
  WHERE mapping = '8 - shared saas runners gpu - medium' --to apply historic medium pl to large
    AND reporting_day <= '2023-06-22'

  UNION ALL

  SELECT DISTINCT
    reporting_day            AS date_day,
    '%-r-saas-l-l-%gpu%'     AS gcp_project_id,
    NULL                     AS gcp_service_description,
    NULL                     AS gcp_sku_description,
    plan_name                AS plan_name, 
    NULL                     AS infra_label,
    NULL                     AS env_label,
    NULL                     AS runner_label,
    NULL                     AS full_path,
    finance_pl_category      AS pl_category,
    pct_ci_minutes           AS pl_percent,
    'ci_runner_pl_daily - 9' AS from_mapping
  FROM final
  WHERE mapping = '9 - shared saas runners gpu - large'

),

runner_saas_macos AS (

  SELECT
    reporting_day                   AS date_day,
    NULL                            AS gcp_project_id,
    NULL                            AS gcp_service_description,
    NULL                            AS gcp_sku_description,
    plan_name                       AS plan_name, 
    NULL                            AS infra_label,
    NULL                            AS env_label,
    '5 - shared saas macos runners' AS runner_label,
    NULL                            AS full_path,
    finance_pl_category             AS pl_category,
    pct_ci_minutes                  AS pl_percent,
    'ci_runner_pl_daily - 5'        AS from_mapping
  FROM final
  WHERE mapping = '5 - shared saas macos runners'

),

runner_saas_private AS (

  SELECT
    reporting_day                  AS date_day,
    NULL                           AS gcp_project_id,
    NULL                           AS gcp_service_description,
    NULL                           AS gcp_sku_description,
    plan_name                      AS plan_name, 
    NULL                           AS infra_label,
    NULL                           AS env_label,
    '6 - private internal runners' AS runner_label,
    NULL                           AS full_path,
    finance_pl_category            AS pl_category,
    pct_ci_minutes                 AS pl_percent,
    'ci_runner_pl_daily - 6'       AS from_mapping
  FROM final
  WHERE mapping = '6 - private internal runners'

),

runner_saas_private_ext AS (

  SELECT DISTINCT
    reporting_day            AS date_day,
    'gitlab-ci-private-_'    AS gcp_project_id,
    NULL                     AS gcp_service_description,
    NULL                     AS gcp_sku_description,
    plan_name                AS plan_name, 
    NULL                     AS infra_label,
    NULL                     AS env_label,
    NULL                     AS runner_label,
    NULL                     AS full_path,
    finance_pl_category      AS pl_category,
    pct_ci_minutes           AS pl_percent,
    'ci_runner_pl_daily - 6' AS from_mapping
  FROM final
  WHERE mapping = '6 - private internal runners'

),

runner_saas_private_ext_saas_l_m AS (

  SELECT DISTINCT
    reporting_day                  AS date_day,
    'gitlab-r-saas-l-m-amd64-org-' AS gcp_project_id,
    NULL                           AS gcp_service_description,
    NULL                           AS gcp_sku_description,
    plan_name                      AS plan_name, 
    NULL                           AS infra_label,
    NULL                           AS env_label,
    NULL                           AS runner_label,
    NULL                           AS full_path,
    finance_pl_category            AS pl_category,
    pct_ci_minutes                 AS pl_percent,
    'ci_runner_pl_daily - 6'       AS from_mapping
  FROM final
  WHERE mapping = '6 - private internal runners'

),

runner_saas_private_ext_saas_l_p AS (

  SELECT DISTINCT
    reporting_day              AS date_day,
    'gitlab-r-saas-l-p-amd64-' AS gcp_project_id,
    NULL                       AS gcp_service_description,
    NULL                       AS gcp_sku_description,
    plan_name                  AS plan_name, 
    NULL                       AS infra_label,
    NULL                       AS env_label,
    NULL                       AS runner_label,
    NULL                       AS full_path,
    finance_pl_category        AS pl_category,
    pct_ci_minutes             AS pl_percent,
    'ci_runner_pl_daily - 6'   AS from_mapping
  FROM final
  WHERE mapping = '6 - private internal runners'

)

SELECT *
FROM runner_shared_gitlab_org
UNION ALL
SELECT *
FROM runner_saas_small
UNION ALL
SELECT *
FROM runner_saas_small_ext
UNION ALL
SELECT *
FROM runner_saas_medium
UNION ALL
SELECT *
FROM runner_saas_medium_ext
UNION ALL
SELECT *
FROM runner_saas_large
UNION ALL
SELECT *
FROM runner_saas_large_ext
UNION ALL
SELECT *
FROM runner_saas_xlarge
UNION ALL
SELECT *
FROM runner_saas_xlarge_ext
UNION ALL
SELECT *
FROM runner_saas_2xlarge
UNION ALL
SELECT *
FROM runner_saas_2xlarge_ext
UNION ALL
SELECT *
FROM runner_saas_medium_gpu
UNION ALL
SELECT *
FROM runner_saas_medium_ext_gpu
UNION ALL
SELECT *
FROM runner_saas_large_gpu
UNION ALL
SELECT *
FROM runner_saas_large_ext_gpu
UNION ALL
SELECT *
FROM runner_saas_macos
UNION ALL
SELECT *
FROM runner_saas_private
UNION ALL
SELECT *
FROM runner_saas_private_ext
UNION ALL
SELECT *
FROM runner_saas_private_ext_saas_l_m
UNION ALL
SELECT *
FROM runner_saas_private_ext_saas_l_p
