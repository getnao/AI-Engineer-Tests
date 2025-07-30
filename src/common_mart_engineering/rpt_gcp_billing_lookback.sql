WITH flex_cud_lookback AS (

  SELECT
    date_day,
    pl_category,
    SUM(net_cost)                                               AS cost,
    RATIO_TO_REPORT(SUM(net_cost)) OVER (PARTITION BY date_day) AS pl_percent
  FROM {{ ref('mart_gcp_billing_line_item') }}
  WHERE gcp_service_description = 'Compute Engine'
    AND (
      LOWER(gcp_sku_description) LIKE '%ram%'
      OR LOWER(gcp_sku_description) LIKE '%core%'
      AND LOWER(gcp_sku_description) NOT LIKE '%commitment%'
      AND LOWER(gcp_sku_description) NOT LIKE '%t2d%'
    )
    AND (usage_unit = 'seconds' OR usage_unit = 'bytes-seconds')
    AND pl_category IS NOT NULL
    AND DATE_TRUNC('month', date_day) >= '2023-02-01'
  GROUP BY 1, 2
  ORDER BY date_day DESC

),

t2d_cud_lookback AS (

  SELECT
    date_day,
    gcp_project_id,
    pl_category,
    SUM(net_cost)                                                               AS cost,
    RATIO_TO_REPORT(SUM(net_cost)) OVER (PARTITION BY date_day, gcp_project_id) AS pl_percent
  FROM {{ ref('mart_gcp_billing_line_item') }}
  WHERE gcp_service_description = 'Compute Engine'
    AND (
      LOWER(gcp_sku_description) LIKE '%ram%'
      OR LOWER(gcp_sku_description) LIKE '%core%'
      AND LOWER(gcp_sku_description) NOT LIKE '%commitment%'
      AND LOWER(gcp_sku_description) LIKE '%t2d%'
    )
    AND (usage_unit = 'seconds' OR usage_unit = 'bytes-seconds')
    AND pl_category IS NOT NULL
    AND DATE_TRUNC('month', date_day) >= '2023-02-01'
  GROUP BY 1, 2, 3
  ORDER BY date_day DESC

),

flex_cud AS (

  SELECT
    date_day,
    'gitlab-production'                             AS gcp_project_id,
    NULL                                            AS gcp_service_description,
    'Commitment - dollar based v1: GCE for 3 years' AS gcp_sku_description,
    'shared'                                        AS infra_label,
    NULL                                            AS env_label,
    NULL                                            AS runner_label,
    NULL                                            AS full_path,
    LOWER(flex_cud_lookback.pl_category)            AS pl_category,
    flex_cud_lookback.pl_percent,
    'flex_cud_lookback'                             AS from_mapping
  FROM flex_cud_lookback

),

t2d_cud AS (

  SELECT
    date_day,
    t2d_cud_lookback.gcp_project_id,
    NULL                                AS gcp_service_description,
    sku_list.sku                        AS gcp_sku_description,
    'shared'                            AS infra_label,
    NULL                                AS env_label,
    NULL                                AS runner_label,
    NULL                                AS full_path,
    LOWER(t2d_cud_lookback.pl_category) AS pl_category,
    t2d_cud_lookback.pl_percent,
    't2d_cud_lookback'                  AS from_mapping
  FROM t2d_cud_lookback
  CROSS JOIN (
    SELECT 'Commitment v1: T2D AMD Cpu in Americas for 3 Year' AS sku
    UNION ALL
    SELECT 'Commitment v1: T2D AMD Ram in Americas for 3 Year'
  ) AS sku_list

)

SELECT *
FROM flex_cud
UNION ALL
SELECT *
FROM t2d_cud
