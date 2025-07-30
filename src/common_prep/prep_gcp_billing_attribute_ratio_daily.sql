WITH gcp_folder_pl_mapping AS (

  SELECT *
  FROM {{ ref ('gcp_billing_folder_pl_mapping') }}
  UNPIVOT (allocation FOR type IN (free, internal, paid))

),

gcp_infralabel_pl_mapping AS (

  SELECT *
  FROM {{ ref ('gcp_billing_infra_pl_mapping') }}
  UNPIVOT (allocation FOR type IN (free, internal, paid))

),

gcp_project_pl_mapping AS (

  SELECT *
  FROM {{ ref ('gcp_billing_project_pl_mapping') }}
  UNPIVOT (allocation FOR type IN (free, internal, paid))

),

date_spine AS (

  SELECT date_day
  FROM {{ ref('prep_date') }}
  WHERE date_day < CURRENT_DATE()
    AND date_day >= '2020-01-01'

),

infralabel_pl AS (

  SELECT
    date_spine.date_day,
    NULL            AS gcp_project_id,
    NULL            AS gcp_service_description,
    NULL            AS gcp_sku_description,
    NULL            AS plan_name,
    infra_label,
    NULL            AS env_label,
    NULL            AS runner_label,
    NULL            AS full_path,
    LOWER(type)     AS pl_category,
    allocation      AS pl_percent,
    'infralabel_pl' AS from_mapping
  FROM gcp_infralabel_pl_mapping
  CROSS JOIN date_spine

),

projects_pl AS (

  SELECT
    date_spine.date_day,
    project_id    AS gcp_project_id,
    NULL          AS gcp_service_description,
    NULL          AS gcp_sku_description,
    NULL          AS plan_name,
    NULL          AS infra_label,
    NULL          AS env_label,
    NULL          AS runner_label,
    NULL          AS full_path,
    LOWER(type)   AS pl_category,
    allocation    AS pl_percent,
    'projects_pl' AS from_mapping
  FROM gcp_project_pl_mapping
  CROSS JOIN date_spine

),

folder_pl AS (

  SELECT
    date_spine.date_day,
    NULL        AS gcp_project_id,
    NULL        AS gcp_service_description,
    NULL        AS gcp_sku_description,
    NULL        AS plan_name,
    NULL        AS infra_label,
    NULL        AS env_label,
    NULL        AS runner_label,
    full_path,
    LOWER(type) AS pl_category,
    allocation  AS pl_percent,
    'folder_pl' AS from_mapping
  FROM gcp_folder_pl_mapping
  CROSS JOIN date_spine

)

SELECT *
FROM infralabel_pl
UNION ALL
SELECT *
FROM projects_pl
UNION ALL
SELECT *
FROM folder_pl
