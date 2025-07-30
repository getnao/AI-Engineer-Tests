WITH cost_data AS (

  SELECT * 
  FROM {{ ref('rpt_gcp_billing_pl_day_ext') }}
  WHERE date_day >= '2023-02-01'

),

ci_minutes AS (

  SELECT
    DATE_TRUNC('day', ci_build_started_at)::DATE AS reporting_day,

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
    END                                          AS pl,
    CASE
      WHEN ci_runner_type_summary = 'shared' THEN 'Shared Runners'
      ELSE 'Self-Managed Runners'
    END                                          AS runner_type,

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
  {{ dbt_utils.group_by(n=5) }}

),

mapped AS (

  SELECT
    reporting_day,
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
  GROUP BY 1, 2, 3
  ORDER BY 1, mapping DESC

),

gitlab_data_ AS (

  SELECT
    reporting_day,
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
  GROUP BY 1, 2, 3


),

cloud_data AS (

  SELECT
    date_day,
    gcp_sku_description,
    level_3,
    level_4,
    CASE
      WHEN (level_3 = 'SaaS' AND level_4 = 'linux small ') THEN '2 - shared saas runners - small'
      WHEN (level_3 = 'SaaS' AND level_4 = 'linux medium') THEN '3 - shared saas runners - medium'
      WHEN (level_3 = 'SaaS' AND level_4 = 'linux large') THEN '4 - shared saas runners - large'
      WHEN (level_3 = 'SaaS' AND level_4 = 'linux xlarge') THEN '10 - shared saas runners - xlarge'
      WHEN (level_3 = 'SaaS' AND level_4 = 'linux 2xlarge') THEN '11 - shared saas runners - 2xlarge'
      WHEN (level_3 = 'SaaS' AND level_4 = 'linux medium gpu') THEN '8 - shared saas runners gpu - medium'
      WHEN (level_3 = 'Internal' AND level_4 = 'linux private internal') THEN '6 - private internal runners'
      WHEN (level_3 = 'Shared org' AND level_4 = 'linux small ') THEN '1 - shared gitlab org runners'
    END                                     AS mapping,
    SUM(usage_amount_in_pricing_units) * 60 AS usage_amount_in_pricing_units,
    SUM(net_cost)                           AS net_cost
  FROM cost_data
  WHERE level_4 IN ('linux small ', 'linux medium', 'linux large', 'linux xlarge', 'linux 2xlarge', 'linux medium gpu', 'linux private internal')
    AND (
      gcp_sku_description LIKE 'N2D AMD Instance Core running in%'
      OR gcp_sku_description LIKE 'N1 Predefined Instance Core running%'
    )
  GROUP BY 1, 2, 3, 4
  ORDER BY 1 DESC

),

gitlab_data AS (

  SELECT
    reporting_day         AS date_day,
    mapping,
    SUM(total_ci_minutes) AS total_ci_minutes
  FROM gitlab_data_
  WHERE reporting_day >= '2023-02-01'
  GROUP BY 1, 2

),

joined AS (

  SELECT
    c.date_day,
    c.level_3,
    c.level_4,
    c.usage_amount_in_pricing_units                      AS cloud_compute_minutes,
    c.net_cost,
    g.total_ci_minutes                                   AS gitlab_ci_minutes,
    c.usage_amount_in_pricing_units - g.total_ci_minutes AS overhead_compute_minutes,
    CASE WHEN c.mapping = '2 - shared saas runners - small' THEN c.usage_amount_in_pricing_units / NULLIFZERO(g.total_ci_minutes * (1 / 2)) -- cost_factor / core_in_machine
      WHEN c.mapping = '3 - shared saas runners - medium' THEN c.usage_amount_in_pricing_units / NULLIFZERO(g.total_ci_minutes * (2 / 4)) -- cost factor = 2, 4 cores
      WHEN c.mapping = '4 - shared saas runners - large' THEN c.usage_amount_in_pricing_units / NULLIFZERO(g.total_ci_minutes * (3 / 8)) -- cost factor = 3, 8 cores
      WHEN c.mapping = '10 - shared saas runners - xlarge' THEN c.usage_amount_in_pricing_units / NULLIFZERO(g.total_ci_minutes * (6 / 16)) -- cost factor = 6, 16 cores
      WHEN c.mapping = '11 - shared saas runners - 2xlarge' THEN c.usage_amount_in_pricing_units / NULLIFZERO(g.total_ci_minutes * (12 / 32)) -- cost factor = 12, 32 cores
      WHEN c.mapping = '8 - shared saas runners gpu - medium' THEN c.usage_amount_in_pricing_units / NULLIFZERO(g.total_ci_minutes * (7 / 4)) -- cost factor = 7, 4 cores
      WHEN c.mapping = '6 - private internal runners' THEN c.usage_amount_in_pricing_units / NULLIFZERO(g.total_ci_minutes * (1 / 2)) -- cost factor none, 2 cores
      WHEN c.mapping = '1 - shared gitlab org runners' THEN c.usage_amount_in_pricing_units / NULLIFZERO(g.total_ci_minutes * (1 / 2)) -- cost factor none, 2 cores
    END                                                  AS compute_efficiency,
    c.net_cost / NULLIFZERO((g.total_ci_minutes / 1000))             AS dollar_efficency_cost_for_1000_ci_minutes
  FROM cloud_data AS c
  LEFT JOIN gitlab_data AS g
    ON c.date_day = g.date_day
      AND c.mapping = g.mapping

)

SELECT * FROM joined
ORDER BY 1 DESC
