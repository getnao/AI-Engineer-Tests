{{ config(

    materialized = "incremental",
    unique_key = ['clean_event_id'],
    full_refresh = only_force_full_refresh(),
    incremental_strategy = "merge_sum",
    merge_sum_columns = ['event_count'],
    on_schema_change = "sync_all_columns",
    tmp_relation_type = "table",
    cluster_by = ['SUBSTRING(clean_event_id, 1, 4)','last_uploaded_at::DATE'],
    automatic_clustering = true,
    snowflake_warehouse=generate_warehouse_name('XL')
) }}

WITH gitlab AS (
  
  SELECT
    IFF(REGEXP_LIKE(event_id, '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'), event_id, NULL) AS clean_event_id,
    uploaded_at
  FROM {{ ref('snowplow_gitlab_good_events_source') }}
  WHERE app_id is NOT NULL
    AND TRY_TO_TIMESTAMP(derived_tstamp) is NOT NULL
    AND clean_event_id IS NOT NULL
  {% if is_incremental() %}
    AND uploaded_at > (SELECT last_uploaded_at FROM {{ this }} ORDER BY last_uploaded_at DESC LIMIT 1)
  {% endif %}

),

counts AS (
  
  SELECT
    clean_event_id,
    COUNT(*) AS event_count,
    MAX(uploaded_at) AS last_uploaded_at
  FROM gitlab
  GROUP BY 1

)
    
SELECT *
FROM counts
