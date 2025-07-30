WITH date_spine AS (

  SELECT date_day
  FROM {{ ref('prep_date') }}
  WHERE date_day < CURRENT_DATE()
    AND date_day >= '2020-01-01'

),

pubsub AS (

  SELECT
    date_day,
    'gitlab-production' AS gcp_project_id,
    'Cloud Pub/Sub'     AS gcp_service_description,
    NULL                AS gcp_sku_description,
    NULL                AS plan_name,
    NULL                AS infra_label,
    NULL                AS env_label,
    NULL                AS runner_label,
    NULL                AS full_path,
    'internal'          AS pl_category,
    1                   AS pl_percent,
    'pubsub internal'   AS from_mapping
  FROM date_spine

)

SELECT *
FROM pubsub
