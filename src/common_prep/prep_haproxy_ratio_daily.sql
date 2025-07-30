WITH haproxy_pl_mapping AS (

  SELECT *
  FROM {{ ref('gcp_billing_haproxy_pl_mapping') }}
  UNPIVOT (allocation FOR type IN (free, internal, paid))

),

haproxy_daily_ratio AS (

  SELECT
    DATE_TRUNC('day', recorded_at)                                      AS date_day,
    backend_category,
    SUM(egress_gibibytes)                                               AS usage_in_gib,
    RATIO_TO_REPORT(SUM(egress_gibibytes)) OVER (PARTITION BY date_day) AS percent_backend_ratio
  FROM {{ ref('ha_proxy_metrics') }}
  {{ dbt_utils.group_by(n=2) }}

),

ha_proxy_ratio_with_pl AS (

  SELECT *
  FROM haproxy_daily_ratio
  INNER JOIN haproxy_pl_mapping
    ON haproxy_daily_ratio.backend_category = haproxy_pl_mapping.metric_backend

),

haproxy_isp AS (

  SELECT
    date_day,
    'gitlab-production'                                           AS gcp_project_id,
    'Compute Engine'                                              AS gcp_service_description,
    'Network Egress via Carrier Peering Network - Americas Based' AS gcp_sku_description,
    NULL                                                          AS plan_name,
    'shared'                                                      AS infra_label,
    NULL                                                          AS env_label,
    NULL                                                          AS runner_label,
    NULL                                                          AS full_path,
    LOWER(type)                                                   AS pl_category,
    percent_backend_ratio * allocation                            AS pl_percent,
    CONCAT('haproxy-cpn-', backend_category)                      AS from_mapping
  FROM ha_proxy_ratio_with_pl

),

haproxy_inter AS (

  SELECT
    date_day,
    'gitlab-production'                               AS gcp_project_id,
    'Compute Engine'                                  AS gcp_service_description,
    'Network Inter Zone Egress'                       AS gcp_sku_description,
    NULL                                              AS plan_name,
    NULL                                              AS infra_label,
    NULL                                              AS env_label,
    NULL                                              AS runner_label,
    NULL                                              AS full_path,
    LOWER(type)                                       AS pl_category,
    percent_backend_ratio * allocation                AS pl_percent,
    CONCAT('haproxy-inter-egress-', backend_category) AS from_mapping
  FROM ha_proxy_ratio_with_pl

  UNION ALL
  SELECT
    date_day,
    'gitlab-production'                               AS gcp_project_id,
    'Compute Engine'                                  AS gcp_service_description,
    'Network Inter Zone Data Transfer Out'            AS gcp_sku_description,
    NULL                                              AS plan_name,
    NULL                                              AS infra_label,
    NULL                                              AS env_label,
    NULL                                              AS runner_label,
    NULL                                              AS full_path,
    LOWER(type)                                       AS pl_category,
    percent_backend_ratio * allocation                AS pl_percent,
    CONCAT('haproxy-inter-egress-', backend_category) AS from_mapping
  FROM ha_proxy_ratio_with_pl

  UNION ALL

  SELECT
    date_day,
    'gitlab-production'                                                      AS gcp_project_id,
    'Compute Engine'                                                         AS gcp_service_description,
    'Network Data Transfer Out via Carrier Peering Network - Americas Based' AS gcp_sku_description,
    NULL                                                                     AS plan_name,
    NULL                                                                     AS infra_label,
    NULL                                                                     AS env_label,
    NULL                                                                     AS runner_label,
    NULL                                                                     AS full_path,
    LOWER(type)                                                              AS pl_category,
    percent_backend_ratio * allocation                                       AS pl_percent,
    CONCAT('haproxy-inter-egress-', backend_category)                        AS from_mapping
  FROM ha_proxy_ratio_with_pl

),

haproxy_cdn AS (

  SELECT
    date_day,
    'gitlab-production'                      AS gcp_project_id,
    'Networking'                             AS gcp_service_description,
    NULL                                     AS gcp_sku_description,
    NULL                                     AS plan_name,
    NULL                                     AS infra_label,
    NULL                                     AS env_label,
    NULL                                     AS runner_label,
    NULL                                     AS full_path,
    LOWER(type)                              AS pl_category,
    percent_backend_ratio * allocation       AS pl_percent,
    CONCAT('haproxy-cdn-', backend_category) AS from_mapping
  FROM ha_proxy_ratio_with_pl

)

SELECT *
FROM haproxy_isp
UNION ALL
SELECT *
FROM haproxy_inter
UNION ALL
SELECT *
FROM haproxy_cdn
