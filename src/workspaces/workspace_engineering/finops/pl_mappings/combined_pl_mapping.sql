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