{{config({
    "materialized":"incremental",
    "unique_key":['event_id', 'derived_tstamp_date'],
    "cluster_by":['derived_tstamp_date'],
    "on_schema_change":"sync_all_columns",
    "full_refresh": only_force_full_refresh(),
    "tmp_relation_type": "view",
    "snowflake_warehouse": generate_warehouse_name('XL')
  })
}}

WITH clean_events as (

    SELECT *
    FROM {{ ref('snowplow_event_counts') }}
    WHERE event_count = 1

),

filtered_source as (

    SELECT
        event_id,
        derived_tstamp::DATE AS derived_tstamp_date,
        TRY_PARSE_JSON(contexts) AS contexts
    {% if target.name not in ("prod") -%}

    FROM {{ ref('snowplow_gitlab_good_events_sample_source') }}

    {%- else %}

    FROM {{ ref('snowplow_gitlab_good_events_source') }}

    {%- endif %}
    INNER JOIN clean_events
      ON event_id = clean_events.clean_event_id
      AND uploaded_at = clean_events.last_uploaded_at
    WHERE true
      AND TRY_TO_TIMESTAMP(derived_tstamp) IS NOT NULL
      AND derived_tstamp >= '{{ get_start_date() }}'  
      AND derived_tstamp < '{{ get_end_date() }}'
    {% if is_incremental() %}

      AND derived_tstamp > (SELECT MAX(derived_tstamp_date)::VARCHAR FROM {{ this }})

    {% endif %}
)

, base AS (

    SELECT *
    FROM filtered_source

)

, column_selection AS (

    SELECT
      base.*,
      -- GitLab Standard Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/gitlab_standard/jsonschema/%',
          context_name='gitlab_standard',
          field_alias_datatype_list=[
            {'field':'environment'},
            {'field':'extra', 'formula':"TRY_PARSE_JSON(gitlab_standard_context['extra'])", 'data_type':'variant'},
            {'field':'namespace_id', 'data_type':'number'},
            {'field':'plan'},
            {'field':'google_analytics_id'},
            {'field':'project_id', 'data_type':'number'},
            {'field':'user_id', 'alias':'pseudonymized_user_id'},
            {'field':'source'},
            {'field':'is_gitlab_team_member',},
            {'field':'feature_enabled_by_namespace_ids'},
            {'field':'instance_id', 'alias':"gsc_instance_id"},
            {'field':'instance_version'},
            {'field':'host_name', 'alias':"gsc_host_name"},
            {'field':'realm', 'alias':"gsc_realm"},
            {'field':'global_user_id'},
            {'field':'correlation_id'},
            {'field':'interface'},
            {'field':'client_type'},
            {'field':'client_name'},
            {'field':'client_version'},
            {'field':'feature_category'},
            {'field':'input_tokens', 'data_type':'int', 'alias':'gsc_input_tokens'},
            {'field':'output_tokens', 'data_type':'int', 'alias':'gsc_output_tokens'},
            {'field':'total_tokens', 'data_type':'int'},
            {'field':'model_engine','alias':'gsc_model_engine'},
            {'field':'model_name','alias':'gsc_model_name'},
            {'field':'model_provider'},
            {'field':'feature_enablement_type'},
            {'field':'unique_instance_id'},
            {'field':'ultimate_parent_namespace_id'},
            {'field':'user_type'}
            ]
        )
      }},
      IFF(google_analytics_id = '', NULL,
          SPLIT_PART(google_analytics_id, '.', 3) || '.' ||
          SPLIT_PART(google_analytics_id, '.', 4))::VARCHAR                                                                                                   AS google_analytics_client_id,
      CASE
        WHEN gsc_realm IN ('SaaS','saas')
          THEN 'SaaS'
        WHEN gsc_realm IN ('Self-Managed','self-managed')
          THEN 'Self-Managed'
        WHEN gsc_realm IS NULL
          THEN NULL
        ELSE gsc_realm
      END                                                                                                                                                     AS gsc_delivery_type,

      -- Web Page Context Columns
       {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/%',
          context_name='web_page',
          field_alias_datatype_list=[
            {'field':'id', 'alias':'web_page_id'}
            ]
        )
      }},

      -- GitLab Experiment Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/gitlab_experiment/jsonschema/%',
          context_name='gitlab_experiment',
          field_alias_datatype_list=[
            {'field':'experiment', 'alias':'experiment_name'},
            {'field':'key', 'alias':'experiment_context_key'},
            {'field':'variant', 'alias':'experiment_variant'},
            {'field':'migration_keys', 'formula':"ARRAY_TO_STRING(gitlab_experiment_context['migration_keys']::VARIANT, ', ')", 'alias':'experiment_migration_keys'}
            ]
        )
      }},

      -- Code Suggestions Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/code_suggestions_context/jsonschema/%',
          context_name='code_suggestions',
          field_alias_datatype_list=[
            {'field':'model_engine'},
            {'field':'model_name'},
            {'field':'prefix_length', 'data_type':'int'},
            {'field':'suffix_length', 'data_type':'int'},
            {'field':'language'},
            {'field':'user_agent'},
            {'field':'gitlab_realm'},
            {'field':'api_status_code', 'data_type':'int'},
            {'field':'gitlab_saas_namespace_ids', 'alias':'saas_namespace_ids'},
            {'field':'gitlab_saas_duo_pro_namespace_ids', 'alias':'duo_namespace_ids'},
            {'field':'gitlab_instance_id', 'alias':'instance_id'},
            {'field':'gitlab_host_name', 'alias':'host_name'},
            {'field':'is_streaming', 'data_type':'boolean'},
            {'field':'gitlab_global_user_id'},
            {'field':'suggestion_source'},
            {'field':'is_invoked', 'data_type':'boolean'},
            {'field':'options_count', 'formula':"NULLIF(code_suggestions_context['options_count']::VARCHAR, 'null')", 'data_type':'number', 'alias':'options_count'},
            {'field':'accepted_option', 'data_type':'int'},
            {'field':'has_advanced_context', 'data_type':'boolean'},
            {'field':'is_direct_connection', 'data_type':'boolean'},
            {'field':'gitlab_instance_version'},
            {'field':'total_context_size_bytes', 'data_type':'int'},
            {'field':'content_above_cursor_size_bytes', 'data_type':'int'},
            {'field':'content_below_cursor_size_bytes', 'data_type':'int'},
            {'field':'context_items', 'data_type':'variant'},
            {'field':'input_tokens', 'data_type':'int'},
            {'field':'output_tokens', 'data_type':'int'},
            {'field':'context_tokens_sent', 'data_type':'int'},
            {'field':'context_tokens_used', 'data_type':'int'},
            {'field':'debounce_interval', 'data_type':'int'},
            {'field':'region'},
            {'field': 'context_items_resolution_strategies_summary', 'alias': 'resolution_strategies', 'data_type':'variant'},
            {'field':'gitlab_feature_enablement_type'}
            ]
        )
      }},
      CASE
        WHEN gitlab_realm IN ('SaaS','saas')
          THEN 'SaaS'
        WHEN gitlab_realm IN ('Self-Managed','self-managed')
          THEN 'Self-Managed'
        WHEN gitlab_realm IS NULL
          THEN NULL
        ELSE gitlab_realm
      END                                                                                                                                                     AS delivery_type,
      COALESCE(
        IFF(duo_namespace_ids = '[]', NULL, duo_namespace_ids),
        IFF(saas_namespace_ids = '[]', NULL, saas_namespace_ids)
        )                                                                                                                                                     AS namespace_ids,

      -- IDE Extension Version Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/ide_extension_version/jsonschema/%',
          context_name='ide_extension_version',
          field_alias_datatype_list=[
            {'field':'extension_name'},
            {'field':'extension_version'},
            {'field':'ide_name'},
            {'field':'ide_vendor'},
            {'field':'ide_version'},
            {'field':'language_server_version'}
            ]
        )
      }},

      -- Service Ping Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/gitlab_service_ping/jsonschema/%',
          context_name='gitlab_service_ping',
          field_alias_datatype_list=[
            {'field':'event_name', 'alias':'redis_event_name'},
            {'field':'key_path'},
            {'field':'data_source'}
            ]
        )
      }},

      -- Performance Timing Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:org.w3/PerformanceTiming/jsonschema/%',
          context_name='performance_timing',
          field_alias_datatype_list=[
            {'field':'connectEnd', 'data_type':'int', 'alias':'connect_end'},
            {'field':'connectStart', 'data_type':'int', 'alias':'connect_start'},
            {'field':'domComplete', 'data_type':'int', 'alias':'dom_complete'},
            {'field':'domContentLoadedEventEnd', 'data_type':'int', 'alias':'dom_content_loaded_event_end'},
            {'field':'domContentLoadedEventStart', 'data_type':'int', 'alias':'dom_content_loaded_event_start'},
            {'field':'domInteractive', 'data_type':'int', 'alias':'dom_interactive'},
            {'field':'domLoading', 'data_type':'int', 'alias':'dom_loading'},
            {'field':'domainLookupEnd', 'data_type':'int', 'alias':'domain_lookup_end'},
            {'field':'domainLookupStart', 'data_type':'int', 'alias':'domain_lookup_start'},
            {'field':'fetchStart', 'data_type':'int', 'alias':'fetch_start'},
            {'field':'loadEventEnd', 'data_type':'int', 'alias':'load_event_end'},
            {'field':'loadEventStart', 'data_type':'int', 'alias':'load_event_start'},
            {'field':'navigationStart', 'data_type':'int', 'alias':'navigation_start'},
            {'field':'redirectEnd', 'data_type':'int', 'alias':'redirect_end'},
            {'field':'redirectStart', 'data_type':'int', 'alias':'redirect_start'},
            {'field':'requestStart', 'data_type':'int', 'alias':'request_start'},
            {'field':'responseEnd', 'data_type':'int', 'alias':'response_end'},
            {'field':'responseStart', 'data_type':'int', 'alias':'response_start'},
            {'field':'secureConnectionStart', 'data_type':'int', 'alias':'secure_connection_start'},
            {'field':'unloadEventEnd', 'data_type':'int', 'alias':'unload_event_end'},
            {'field':'unloadEventStart', 'data_type':'int', 'alias':'unload_event_start'}
            ]
        )
      }}

    FROM base

)

SELECT 
  * EXCLUDE (instance_version,gitlab_instance_version,gsc_delivery_type,delivery_type,gsc_instance_id,gsc_host_name,host_name,instance_id,global_user_id,gitlab_global_user_id,gsc_model_engine, model_engine,gsc_model_name, model_name,gsc_input_tokens, input_tokens,gsc_output_tokens, output_tokens, gitlab_feature_enablement_type, feature_enablement_type),
  COALESCE(instance_version, gitlab_instance_version)               AS instance_version,
  COALESCE(gsc_delivery_type, delivery_type)                        AS delivery_type,
  COALESCE(gsc_instance_id, instance_id)                            AS instance_id,
  COALESCE(gsc_host_name, host_name)                                AS host_name,
  COALESCE(global_user_id, gitlab_global_user_id)                   AS gitlab_global_user_id,
  ARRAY_SIZE(context_items)                                         AS context_items_count,
  COALESCE(gsc_model_engine, model_engine)                          AS model_engine,
  COALESCE(gsc_model_name, model_name)                              AS model_name,
  COALESCE(gsc_input_tokens, input_tokens)                          AS input_tokens,
  COALESCE(gsc_output_tokens, output_tokens)                        AS output_tokens,
  COALESCE(feature_enablement_type, gitlab_feature_enablement_type) AS feature_enablement_type
FROM column_selection
