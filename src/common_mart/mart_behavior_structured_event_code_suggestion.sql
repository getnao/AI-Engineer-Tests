{{ config(
    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=['mnpi_exception','product'],
    full_refresh= only_force_full_refresh(),
    on_schema_change='sync_all_columns',
    snowflake_warehouse=generate_warehouse_name('XL')
  ) }}

{{ simple_cte([
    ('fct_behavior_structured_event_code_suggestion', 'fct_behavior_structured_event_code_suggestion'),
    ('dim_behavior_event', 'dim_behavior_event'),
    ('dim_crm_account', 'dim_crm_account')
]) }}
,
{% if is_incremental() %}

{% set check_changes_query %}

  -- Check if any account names have changed since last run
  -- This lets us avoid the complex incremental condition when not needed
  SELECT COUNT(*) > 0 AS has_changes
  FROM {{ ref('dim_crm_account') }} 
  INNER JOIN {{this}} AS existing
    ON dim_crm_account.dim_crm_account_id = existing.dim_crm_account_id
  WHERE
    (
      COALESCE(dim_crm_account.crm_account_name, '') != COALESCE(existing.crm_account_name, '')
      OR COALESCE(dim_crm_account.parent_crm_account_name, '') != COALESCE(existing.parent_crm_account_name, '')
    )
{% endset %}

{% set changes_result = run_query(check_changes_query) %}
{% set has_changes = changes_result.columns[0].values()[0] %}

{% if has_changes %}

-- For accounts with name changes, identify the specific event records that need updating
records_to_update AS (
  SELECT DISTINCT
    existing.behavior_structured_event_pk
  FROM dim_crm_account
  INNER JOIN {{this}} AS existing
    ON dim_crm_account.dim_crm_account_id = existing.dim_crm_account_id
  WHERE
    (
      COALESCE(dim_crm_account.crm_account_name, '') != COALESCE(existing.crm_account_name, '')
      OR COALESCE(dim_crm_account.parent_crm_account_name, '') != COALESCE(existing.parent_crm_account_name, '')
    )
),
{% endif %}
{% endif %}

code_suggestions_context AS (

  SELECT
    {{ dbt_utils.star(from=ref('fct_behavior_structured_event_code_suggestion'), except=["CREATED_BY",
    "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
  FROM fct_behavior_structured_event_code_suggestion
  WHERE behavior_at >= '2023-08-25'
    AND has_code_suggestions_context = TRUE
    AND app_id IN (
      'gitlab_ai_gateway', 
      'gitlab_ide_extension',
      'gitlab_ide_extension_sm',
      'gitlab_ide_extension_dedicated'
    ) --"official" Code Suggestions app_ids
    AND NOT (COALESCE(ide_name, '') = 'Visual Studio Code' AND COALESCE(extension_version, '') = '3.76.0')
    {% if is_incremental() %}
      AND (
        behavior_at >= (SELECT MAX(behavior_at) FROM {{ this }})
        {% if has_changes %}
        -- Only includes the OR condition when account names have changed
        OR behavior_structured_event_pk IN (SELECT behavior_structured_event_pk FROM records_to_update)
        {% endif %}
      )
    {% endif %}

),

flattened_ids AS (
  SELECT
    behavior_structured_event_pk,
    f.value AS dim_crm_account_id
  FROM code_suggestions_context,
    LATERAL FLATTEN(input => dim_crm_account_ids) AS f
  WHERE dim_crm_account_ids IS NOT NULL
    AND ARRAY_SIZE(dim_crm_account_ids) > 0
),

account_names AS (
  SELECT
    flattened_ids.behavior_structured_event_pk,
    dim_crm_account.crm_account_name,
    dim_crm_account.parent_crm_account_name
  FROM flattened_ids
  LEFT JOIN dim_crm_account
    ON flattened_ids.dim_crm_account_id = dim_crm_account.dim_crm_account_id
),

derived_account_names AS (

  SELECT
    behavior_structured_event_pk,
    ARRAY_UNIQUE_AGG(crm_account_name)        AS crm_account_names,
    ARRAY_UNIQUE_AGG(parent_crm_account_name) AS parent_crm_account_names,
    ARRAY_SIZE(crm_account_names)             AS count_crm_account_names,
    ARRAY_SIZE(parent_crm_account_names)      AS count_parent_crm_account_names,
    IFF(
      count_crm_account_names = 1,
      GET(crm_account_names, 0)::VARCHAR, NULL
    )                                         AS crm_account_name,
    IFF(
      count_parent_crm_account_names = 1,
      GET(parent_crm_account_names, 0)::VARCHAR, NULL
    )                                         AS parent_crm_account_name
  FROM account_names
  GROUP BY behavior_structured_event_pk
),

filtered_code_suggestion_events AS (

  SELECT
    code_suggestions_context.behavior_structured_event_pk,
    code_suggestions_context.behavior_at,
    code_suggestions_context.behavior_at::DATE                   AS behavior_date,
    code_suggestions_context.app_id,
    dim_behavior_event.event_category,
    dim_behavior_event.event_action,
    dim_behavior_event.event_label,
    dim_behavior_event.event_property,
    code_suggestions_context.language,
    code_suggestions_context.delivery_type,
    code_suggestions_context.model_engine,
    code_suggestions_context.model_name,
    code_suggestions_context.prefix_length,
    code_suggestions_context.suffix_length,
    code_suggestions_context.api_status_code,
    code_suggestions_context.extension_name,
    code_suggestions_context.extension_version,
    code_suggestions_context.ide_name,
    code_suggestions_context.ide_vendor,
    code_suggestions_context.ide_version,
    code_suggestions_context.language_server_version,
    code_suggestions_context.contexts,
    code_suggestions_context.code_suggestions_context,
    code_suggestions_context.ide_extension_version_context,
    code_suggestions_context.has_code_suggestions_context,
    code_suggestions_context.has_ide_extension_version_context,
    code_suggestions_context.dim_instance_id,
    code_suggestions_context.unique_instance_id,
    code_suggestions_context.host_name,
    code_suggestions_context.is_streaming,
    code_suggestions_context.gitlab_global_user_id,
    code_suggestions_context.suggestion_source,
    code_suggestions_context.is_invoked,
    code_suggestions_context.options_count,
    code_suggestions_context.accepted_option,
    code_suggestions_context.has_advanced_context,
    code_suggestions_context.is_direct_connection,
    code_suggestions_context.namespace_ids,
    code_suggestions_context.ultimate_parent_namespace_ids,
    code_suggestions_context.dim_installation_ids,
    code_suggestions_context.host_names,
    code_suggestions_context.subscription_names,
    code_suggestions_context.dim_crm_account_ids,
    COALESCE(derived_account_names.crm_account_names, [])        AS crm_account_names,
    COALESCE(derived_account_names.parent_crm_account_names, []) AS parent_crm_account_names,
    derived_account_names.crm_account_name,
    derived_account_names.parent_crm_account_name,
    code_suggestions_context.dim_parent_crm_account_ids,
    code_suggestions_context.dim_crm_account_id,
    code_suggestions_context.dim_parent_crm_account_id,
    code_suggestions_context.subscription_name,
    code_suggestions_context.ultimate_parent_namespace_id,
    code_suggestions_context.dim_installation_id,
    code_suggestions_context.installation_host_name,
    code_suggestions_context.product_deployment_type,
    code_suggestions_context.namespace_is_internal,
    code_suggestions_context.installation_is_internal,
    code_suggestions_context.gsc_instance_version,
    code_suggestions_context.total_context_size_bytes,
    code_suggestions_context.content_above_cursor_size_bytes,
    code_suggestions_context.content_below_cursor_size_bytes,
    code_suggestions_context.context_items,
    code_suggestions_context.context_items_count,
    code_suggestions_context.input_tokens,
    code_suggestions_context.output_tokens,
    code_suggestions_context.context_tokens_sent,
    code_suggestions_context.context_tokens_used,
    code_suggestions_context.debounce_interval,
    code_suggestions_context.region,
    code_suggestions_context.resolution_strategies,
    code_suggestions_context.feature_enablement_type
  FROM code_suggestions_context
  LEFT JOIN dim_behavior_event
    ON code_suggestions_context.dim_behavior_event_sk = dim_behavior_event.dim_behavior_event_sk
  LEFT JOIN derived_account_names
    ON code_suggestions_context.behavior_structured_event_pk = derived_account_names.behavior_structured_event_pk

)

SELECT *
FROM filtered_code_suggestion_events
