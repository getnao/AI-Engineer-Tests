{{ config(
    materialized = 'incremental',
    unique_key = "event_id",
    post_hook =["{{ rolling_window_delete('behavior_at','month', 25) }}"],
    on_schema_change = "sync_all_columns",
    cluster_by = ['behavior_at::DATE'],
    automatic_clustering = true,
    snowflake_warehouse = generate_warehouse_name('XL')
) }}


{% set change_form = ['formId','elementId','nodeName','type','elementClasses','value'] %}
{% set submit_form = ['formId','formClasses','elements'] %}
{% set focus_form = ['formId','elementId','nodeName','elementType','elementClasses','value'] %}
{% set link_click = ['elementId','elementClasses','elementTarget','targetUrl','elementContent'] %}
{% set track_timing = ['category','variable','timing','label'] %}


WITH clean_events as (

    SELECT *
    FROM {{ ref('snowplow_event_counts') }}
    WHERE event_count = 1

),


filtered_source as (

    SELECT 
      snowplow_gitlab_good_events_sample_source.* EXCLUDE(contexts),
      TRY_PARSE_JSON(contexts)                                          AS contexts
    FROM {{ ref('snowplow_gitlab_good_events_sample_source') }}
    INNER JOIN clean_events
      ON event_id = clean_events.clean_event_id
      AND uploaded_at = clean_events.last_uploaded_at
    {% if is_incremental() %}

      AND derived_tstamp > (SELECT MAX(behavior_at)::VARCHAR FROM {{this}})

    {% endif %}

), 

renaming AS (

    SELECT 
      -- Most data types taken from https://docs.snowplow.io/docs/fundamentals/canonical-event/
      app_id::VARCHAR AS app_id,
      br_colordepth::VARCHAR                                                                                 AS browser_color_depth,
      br_cookies::BOOLEAN                                                                                    AS has_browser_cookies,
      br_family::VARCHAR                                                                                     AS browser_name,
      br_features_director::BOOLEAN                                                                          AS has_browser_features_director,
      br_features_flash::BOOLEAN                                                                             AS has_browser_features_flash,
      br_features_gears::BOOLEAN                                                                             AS has_browser_features_gears,
      br_features_java::BOOLEAN                                                                              AS has_browser_features_java,
      br_features_pdf::BOOLEAN                                                                               AS has_browser_features_pdf,
      br_features_quicktime::BOOLEAN                                                                         AS has_browser_features_quicktime,
      br_features_realplayer::BOOLEAN                                                                        AS has_browser_features_realplayer,
      br_features_silverlight::BOOLEAN                                                                       AS has_browser_features_silverlight,
      br_features_windowsmedia::BOOLEAN                                                                      AS has_browser_features_windowsmedia,
      br_lang::VARCHAR                                                                                       AS browser_language,
      br_name::VARCHAR                                                                                       AS browser_major_version,
      br_renderengine::VARCHAR                                                                               AS browser_engine,
      br_type::VARCHAR                                                                                       AS browser_type,
      br_version::VARCHAR                                                                                    AS browser_minor_version,
      br_viewheight::INT                                                                                     AS browser_view_height,
      br_viewwidth::INT                                                                                      AS browser_view_width,
      collector_tstamp::TIMESTAMP                                                                            AS collector_timestamp,
      contexts::VARIANT                                                                                      AS contexts,
      doc_charset::VARCHAR                                                                                   AS doc_charset,
      TRY_TO_TIMESTAMP(derived_tstamp)                                                                       AS derived_timestamp,
      TRY_TO_NUMERIC(doc_height)                                                                             AS doc_height,
      TRY_TO_NUMERIC(doc_width)                                                                              AS doc_width,
      domain_sessionid::VARCHAR                                                                              AS session_id,
      domain_sessionidx::INT                                                                                 AS session_index,
      domain_userid::VARCHAR                                                                                 AS user_snowplow_domain_id,
      dvce_created_tstamp::TIMESTAMP                                                                         AS device_created_timestamp,
      dvce_ismobile::BOOLEAN                                                                                 AS is_device_mobile,
      dvce_screenheight::INT                                                                                 AS device_screen_height,
      dvce_screenwidth::INT                                                                                  AS device_screen_width,
      dvce_sent_tstamp::TIMESTAMP                                                                            AS device_sent_timestamp,
      dvce_type::VARCHAR                                                                                     AS device_type,
      etl_tstamp::TIMESTAMP                                                                                  AS etl_timestamp,
      event::VARCHAR                                                                                         AS event_type,
      event_format::VARCHAR                                                                                  AS event_format,
      event_id::VARCHAR                                                                                      AS event_id,
      event_name::VARCHAR                                                                                    AS event_name,
      event_vendor::VARCHAR                                                                                  AS event_vendor,
      event_version::VARCHAR                                                                                 AS event_version,
      IFNULL(geo_city, 'Unknown')::VARCHAR                                                                   AS user_city,
      IFNULL(geo_country, 'Unknown')::VARCHAR                                                                AS user_country,
      IFNULL(geo_region, 'Unknown')::VARCHAR                                                                 AS user_region,
      IFNULL(geo_region_name, 'Unknown')::VARCHAR                                                            AS user_region_name,
      IFNULL(geo_timezone, 'Unknown')::VARCHAR                                                               AS user_timezone_name,
      name_tracker::VARCHAR                                                                                  AS name_tracker,
      network_userid::VARCHAR                                                                                AS network_user_id,
      os_family::VARCHAR                                                                                     AS os,
      os_manufacturer::VARCHAR                                                                               AS os_manufacturer,
      os_name::VARCHAR                                                                                       AS os_name,
      os_timezone::VARCHAR                                                                                   AS os_timezone,
      page_referrer::VARCHAR                                                                                 AS referrer_url,
      page_title::VARCHAR                                                                                    AS page_title,
      page_url::VARCHAR                                                                                      AS page_url,
      page_urlfragment::VARCHAR                                                                              AS page_url_fragment,
      page_urlhost::VARCHAR                                                                                  AS page_url_host,
      page_urlpath::VARCHAR                                                                                  AS page_url_path,
      page_urlport::VARCHAR                                                                                  AS page_url_port,
      page_urlquery::VARCHAR                                                                                 AS page_url_query,
      page_urlscheme::VARCHAR                                                                                AS page_url_scheme,
      platform::VARCHAR                                                                                      AS platform,
      TRY_TO_NUMERIC(pp_xoffset_max)                                                                         AS pp_xoffset_max,
      TRY_TO_NUMERIC(pp_xoffset_min)                                                                         AS pp_xoffset_min,
      TRY_TO_NUMERIC(pp_yoffset_max)                                                                         AS pp_yoffset_max,
      TRY_TO_NUMERIC(pp_yoffset_min)                                                                         AS pp_yoffset_min,
      refr_urlfragment::VARCHAR                                                                              AS referrer_url_fragment,
      refr_urlhost::VARCHAR                                                                                  AS referrer_url_host,
      refr_urlpath::VARCHAR                                                                                  AS referrer_url_path,
      refr_urlport::VARCHAR                                                                                  AS referrer_url_port,
      refr_urlquery::VARCHAR                                                                                 AS referrer_url_query,
      refr_urlscheme::VARCHAR                                                                                AS referrer_url_scheme,
      se_action::VARCHAR                                                                                     AS event_action,
      se_category::VARCHAR                                                                                   AS event_category,
      se_label::VARCHAR                                                                                      AS event_label,
      se_property::VARCHAR                                                                                   AS event_property,
      se_value::DECIMAL                                                                                      AS event_value,
      unstruct_event::VARIANT                                                                                AS unstructured_event,
      user_fingerprint::INT                                                                                  AS user_fingerprint,
      useragent::VARCHAR                                                                                     AS useragent,
      v_collector::VARCHAR                                                                                   AS collector_version,
      v_etl::VARCHAR                                                                                         AS etl_version,
      v_tracker::VARCHAR                                                                                     AS tracker_version,
      uploaded_at::TIMESTAMP                                                                                 AS uploaded_at
    FROM filtered_source


), 

context_flattening AS (

    SELECT 

      renaming.*,

      /*
      This will eventually be replaced with a new macro that automatically pulls all columns 
      from the contexts and flattens them.

      For now, we will maintain the current macro logic where new columns must be identified and added manually.

      This will require a backfill each time a new column is added to this CTE until it is replaced.

      */

      -- GitLab Standard Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/gitlab_standard/jsonschema/%',
          context_name='gitlab_standard',
          field_alias_datatype_list=[
            {'field':'environment','alias':'gitlab_standard_environment'},
            {'field':'extra', 'formula':"TRY_PARSE_JSON(gitlab_standard_context['extra'])", 'data_type':'variant','alias':'gitlab_standard_extra'},
            {'field':'namespace_id', 'data_type':'number','alias':'gitlab_standard_namespace_id'},
            {'field':'plan','alias':'gitlab_standard_plan'},
            {'field':'google_analytics_id','alias':'gitlab_standard_google_analytics_id'},
            {'field':'project_id', 'data_type':'number','alias':'gitlab_standard_project_id'},
            {'field':'user_id', 'alias':'gitlab_standard_user_id'},
            {'field':'source','alias':'gitlab_standard_source'},
            {'field':'is_gitlab_team_member','alias':'gitlab_standard_is_gitlab_team_member'},
            {'field':'feature_enabled_by_namespace_ids','alias':'gitlab_standard_feature_enabled_by_namespace_ids'},
            {'field':'instance_id', 'alias':'gitlab_standard_instance_id'},
            {'field':'instance_version','alias':'gitlab_standard_instance_version'},
            {'field':'host_name', 'alias':'gitlab_standard_host_name'},
            {'field':'realm', 'alias':'gitlab_standard_realm'},
            {'field':'global_user_id','alias':'gitlab_standard_global_user_id'},
            {'field':'correlation_id','alias':'gitlab_standard_correlation_id'},
            {'field':'interface','alias':'gitlab_standard_interface'},
            {'field':'client_type','alias':'gitlab_standard_client_type'},
            {'field':'client_name','alias':'gitlab_standard_client_name'},
            {'field':'client_version','alias':'gitlab_standard_client_version'},
            {'field':'feature_category','alias':'gitlab_standard_feature_category'},
            {'field':'input_tokens', 'data_type':'int', 'alias':'gitlab_standard_input_tokens'},
            {'field':'output_tokens', 'data_type':'int', 'alias':'gitlab_standard_output_tokens'},
            {'field':'total_tokens', 'data_type':'int','alias':'gitlab_standard_total_tokens'},
            {'field':'model_engine','alias':'gitlab_standard_model_engine'},
            {'field':'model_name','alias':'gitlab_standard_model_name'},
            {'field':'model_provider','alias':'gitlab_standard_model_provider'},
            {'field':'feature_enablement_type','alias':'gitlab_standard_feature_enablement_type'},
            {'field':'unique_instance_id','alias':'gitlab_standard_unique_instance_id'},
            {'field':'ultimate_parent_namespace_id','alias':'gitlab_standard_ultimate_parent_namespace_id'}
            ]
        )
      }},

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
            {'field':'experiment', 'alias':'gitlab_experiment_name'},
            {'field':'key', 'alias':'gitlab_experiment_context_key'},
            {'field':'variant', 'alias':'gitlab_experiment_variant'},
            {'field':'migration_keys', 'formula':"ARRAY_TO_STRING(gitlab_experiment_context['migration_keys']::VARIANT, ', ')", 'alias':'gitlab_experiment_migration_keys'}
            ]
        )
      }},

      -- Code Suggestions Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/code_suggestions_context/jsonschema/%',
          context_name='code_suggestions',
          field_alias_datatype_list=[
            {'field':'model_engine', 'alias':'code_suggestions_context_model_engine'},
            {'field':'model_name', 'alias':'code_suggestions_context_model_name'},
            {'field':'prefix_length', 'data_type':'int', 'alias':'code_suggestions_context_prefix_length'},
            {'field':'suffix_length', 'data_type':'int', 'alias':'code_suggestions_context_suffix_length'},
            {'field':'language', 'alias':'code_suggestions_context_language'},
            {'field':'user_agent', 'alias':'code_suggestions_context_user_agent'},
            {'field':'gitlab_realm', 'alias':'code_suggestions_context_gitlab_realm'},
            {'field':'api_status_code', 'data_type':'int', 'alias':'code_suggestions_context_api_status_code'},
            {'field':'gitlab_saas_namespace_ids', 'alias':'code_suggestions_context_gitlab_saas_namespace_ids'},
            {'field':'gitlab_saas_duo_pro_namespace_ids', 'alias':'code_suggestions_context_gitlab_saas_duo_pro_namespace_ids'},
            {'field':'gitlab_instance_id', 'alias':'code_suggestions_context_gitlab_instance_id'},
            {'field':'gitlab_host_name', 'alias':'code_suggestions_context_gitlab_host_name'},
            {'field':'is_streaming', 'data_type':'boolean', 'alias':'code_suggestions_context_is_streaming'},
            {'field':'gitlab_global_user_id', 'alias':'code_suggestions_context_gitlab_global_user_id'},
            {'field':'suggestion_source', 'alias':'code_suggestions_context_suggestion_source'},
            {'field':'is_invoked', 'data_type':'boolean', 'alias':'code_suggestions_context_is_invoked'},
            {'field':'options_count', 'formula':"NULLIF(code_suggestions_context['options_count']::VARCHAR, 'null')", 'data_type':'number', 'alias':'code_suggestions_context_options_count'},
            {'field':'accepted_option', 'data_type':'int', 'alias':'code_suggestions_context_accepted_option'},
            {'field':'has_advanced_context', 'data_type':'boolean', 'alias':'code_suggestions_context_has_advanced_context'},
            {'field':'is_direct_connection', 'data_type':'boolean', 'alias':'code_suggestions_context_is_direct_connection'},
            {'field':'gitlab_instance_version', 'alias':'code_suggestions_context_gitlab_instance_version'},
            {'field':'total_context_size_bytes', 'data_type':'int', 'alias':'code_suggestions_context_total_context_size_bytes'},
            {'field':'content_above_cursor_size_bytes', 'data_type':'int', 'alias':'code_suggestions_context_content_above_cursor_size_bytes'},
            {'field':'content_below_cursor_size_bytes', 'data_type':'int', 'alias':'code_suggestions_context_content_below_cursor_size_bytes'},
            {'field':'context_items', 'data_type':'variant', 'alias':'code_suggestions_context_context_items'},
            {'field':'input_tokens', 'data_type':'int', 'alias':'code_suggestions_context_input_tokens'},
            {'field':'output_tokens', 'data_type':'int', 'alias':'code_suggestions_context_output_tokens'},
            {'field':'context_tokens_sent', 'data_type':'int', 'alias':'code_suggestions_context_context_tokens_sent'},
            {'field':'context_tokens_used', 'data_type':'int', 'alias':'code_suggestions_context_context_tokens_used'},
            {'field':'debounce_interval', 'data_type':'int', 'alias':'code_suggestions_context_debounce_interval'},
            {'field':'region', 'alias':'code_suggestions_context_region'},
            {'field': 'context_items_resolution_strategies_summary', 'data_type':'variant', 'alias': 'code_suggestions_context_context_items_resolution_strategies_summary'},
            {'field':'gitlab_feature_enablement_type', 'alias':'code_suggestions_context_gitlab_feature_enablement_type'}
            ]
        )
      }},

      -- IDE Extension Version Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/ide_extension_version/jsonschema/%',
          context_name='ide_extension_version',
          field_alias_datatype_list=[
            {'field':'extension_name', 'alias':'ide_extension_version_extension_name'},
            {'field':'extension_version', 'alias':'ide_extension_version_extension_version'},
            {'field':'ide_name', 'alias':'ide_extension_version_ide_name'},
            {'field':'ide_vendor', 'alias':'ide_extension_version_ide_vendor'},
            {'field':'ide_version', 'alias':'ide_extension_version_ide_version'},
            {'field':'language_server_version', 'alias':'ide_extension_version_language_server_version'}
            ]
        )
      }},

      -- Service Ping Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/gitlab_service_ping/jsonschema/%',
          context_name='gitlab_service_ping',
          field_alias_datatype_list=[
            {'field':'event_name', 'alias':'gitlab_service_ping_event_name'},
            {'field':'key_path', 'alias':'gitlab_service_ping_key_path'},
            {'field':'data_source', 'alias':'gitlab_service_ping_data_source'}
            ]
        )
      }},

      -- Performance Timing Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:org.w3/PerformanceTiming/jsonschema/%',
          context_name='performance_timing',
          field_alias_datatype_list=[
            {'field':'connectEnd', 'data_type':'int', 'alias':'performance_timing_connectEnd'},
            {'field':'connectStart', 'data_type':'int', 'alias':'performance_timing_connectStart'},
            {'field':'domComplete', 'data_type':'int', 'alias':'performance_timing_domComplete'},
            {'field':'domContentLoadedEventEnd', 'data_type':'int', 'alias':'performance_timing_domContentLoadedEventEnd'},
            {'field':'domContentLoadedEventStart', 'data_type':'int', 'alias':'performance_timing_domContentLoadedEventStart'},
            {'field':'domInteractive', 'data_type':'int', 'alias':'performance_timing_domInteractive'},
            {'field':'domLoading', 'data_type':'int', 'alias':'performance_timing_domLoading'},
            {'field':'domainLookupEnd', 'data_type':'int', 'alias':'performance_timing_domainLookupEnd'},
            {'field':'domainLookupStart', 'data_type':'int', 'alias':'performance_timing_domainLookupStart'},
            {'field':'fetchStart', 'data_type':'int', 'alias':'performance_timing_fetchStart'},
            {'field':'loadEventEnd', 'data_type':'int', 'alias':'performance_timing_loadEventEnd'},
            {'field':'loadEventStart', 'data_type':'int', 'alias':'performance_timing_loadEventStart'},
            {'field':'navigationStart', 'data_type':'int', 'alias':'performance_timing_navigationStart'},
            {'field':'redirectEnd', 'data_type':'int', 'alias':'performance_timing_redirectEnd'},
            {'field':'redirectStart', 'data_type':'int', 'alias':'performance_timing_redirectStart'},
            {'field':'requestStart', 'data_type':'int', 'alias':'performance_timing_requestStart'},
            {'field':'responseEnd', 'data_type':'int', 'alias':'performance_timing_responseEnd'},
            {'field':'responseStart', 'data_type':'int', 'alias':'performance_timing_responseStart'},
            {'field':'secureConnectionStart', 'data_type':'int', 'alias':'performance_timing_secureConnectionStart'},
            {'field':'unloadEventEnd', 'data_type':'int', 'alias':'performance_timing_unloadEventEnd'},
            {'field':'unloadEventStart', 'data_type':'int', 'alias':'performance_timing_unloadEventStart'}
            ]
        )
      }},
      {{ unpack_unstructured_event('unstructured_event', change_form, 'change_form', 'change_form') }},
      {{ unpack_unstructured_event('unstructured_event', submit_form, 'submit_form', 'submit_form') }},
      {{ unpack_unstructured_event('unstructured_event', focus_form, 'focus_form', 'focus_form') }},
      {{ unpack_unstructured_event('unstructured_event', link_click, 'link_click', 'link_click') }},
      {{ unpack_unstructured_event('unstructured_event', track_timing, 'track_timing', 'track_timing') }}

    FROM renaming

),

  derived_fields AS (

    SELECT 

      context_flattening.* EXCLUDE(os_timezone, is_device_mobile),

      -- Derived Fields
      COALESCE(gitlab_standard_instance_version, code_suggestions_context_gitlab_instance_version)                                                                     AS instance_version,
      COALESCE(gitlab_standard_instance_id, code_suggestions_context_gitlab_instance_id)                                                                               AS dim_instance_id,
      COALESCE(gitlab_standard_host_name, code_suggestions_context_gitlab_host_name)                                                                                   AS host_name,
      COALESCE(gitlab_standard_global_user_id, code_suggestions_context_gitlab_global_user_id)                                                                         AS gitlab_global_user_id,
      COALESCE(gitlab_standard_model_engine, code_suggestions_context_model_engine)                                                                                    AS model_engine,
      COALESCE(gitlab_standard_model_name, code_suggestions_context_model_name)                                                                                        AS model_name,
      COALESCE(gitlab_standard_input_tokens, code_suggestions_context_input_tokens)                                                                                    AS input_tokens,
      COALESCE(gitlab_standard_output_tokens, code_suggestions_context_output_tokens)                                                                                  AS output_tokens,
      COALESCE(gitlab_standard_feature_enablement_type, code_suggestions_context_gitlab_feature_enablement_type)                                                       AS feature_enablement_type,
      COALESCE(gitlab_standard_realm, code_suggestions_context_gitlab_realm)                                                                                           AS realm,
      ARRAY_SIZE(code_suggestions_context_context_items)                                                                                                               AS context_items_count,
      CASE
        WHEN realm IN ('SaaS','saas')
          THEN 'SaaS'
        WHEN realm IN ('Self-Managed','self-managed')
          THEN 'Self-Managed'
        WHEN realm IS NULL
          THEN NULL
        ELSE realm
      END                                                                                                                                                              AS product_delivery_type,
      IFF(gitlab_standard_google_analytics_id = '', 
          NULL,
          SPLIT_PART(gitlab_standard_google_analytics_id, '.', 3) || '.' || SPLIT_PART(gitlab_standard_google_analytics_id, '.', 4)
          )::VARCHAR                                                                                                                                                  AS google_analytics_client_id,
      COALESCE(
        IFF(code_suggestions_context_gitlab_saas_duo_pro_namespace_ids = '[]', NULL, code_suggestions_context_gitlab_saas_duo_pro_namespace_ids),
        IFF(code_suggestions_context_gitlab_saas_namespace_ids = '[]', NULL, code_suggestions_context_gitlab_saas_namespace_ids)
        )                                                                                                                                                              AS namespace_ids,

      IFF(REGEXP_LIKE(event_label, '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'),
                                          'identifier_containing_numbers', event_label)                                                                                 AS clean_event_label,
      {{ clean_url('page_url_path') }}                                                                                                                                  AS clean_url_path,
      REPLACE(
          CASE
            WHEN os_timezone = 'Asia/Calcutt' THEN 'Asia/Calcutta'
            WHEN os_timezone = 'Asia/Rangoo' THEN 'Asia/Rangoon'
            WHEN os_timezone = 'Asia/Shangh' THEN 'Asia/Shanghai'
            WHEN os_timezone = 'America/Buenos_Airesnos_Aires' THEN 'America/Buenos_Aires'
            WHEN os_timezone = 'Asia/SaigonMinh' THEN 'Asia/Ho_Chi_Minh'
            WHEN os_timezone = 'Asia/Singaporen27lirczxx' THEN 'Asia/Singapore'
            WHEN os_timezone = 'Etc/Unknown' THEN NULL
            WHEN os_timezone = 'America/A' THEN NULL
            WHEN os_timezone = 'SystemV/EST5' THEN NULL
            WHEN os_timezone = 'SystemV/HST10' THEN NULL
            WHEN os_timezone = 'EuropeALondon' THEN 'Europe/London'
            WHEN os_timezone = 'SystemV/CST6' THEN NULL
            WHEN os_timezone = 'America/New_York01ix691pvh' THEN NULL
            WHEN os_timezone = 'America/Coyhaique' THEN NULL
            WHEN os_timezone = '9072hrct3fqlaw0xcgxjbdbltczan6sukm8evij7' THEN NULL
            WHEN os_timezone = 'cbbkv8gayz' THEN NULL
            WHEN REGEXP_INSTR(os_timezone, 'oastify.com') != 0 THEN NULL
            WHEN os_timezone = 'SystemV/MST7' THEN NULL
            WHEN os_timezone = 'UTC' THEN 'UTC'
            WHEN LEFT(os_timezone, 13) = 'Europe/Zurich' THEN 'Europe/Zurich'
            WHEN NOT REGEXP_LIKE(os_timezone,'^[A-Za-z0-9_+-]+\/[A-Za-z0-9_+-]+(\/[A-Za-z0-9_+-]+)*$') THEN NULL
            ELSE os_timezone
          END,
        '%2F', '/'
        )                                                                                                                                                   AS os_timezone,
      IFF(
        DATE_PART('year', derived_timestamp) > 1970,
        derived_timestamp, 
        collector_timestamp
        )::TIMESTAMP                                                                                                                                        AS behavior_at,
      IFF(device_type = 'Tablet' AND is_device_mobile::BOOLEAN = TRUE, FALSE, is_device_mobile::BOOLEAN)::BOOLEAN                                           AS is_device_mobile,
      /* Deployment type:

      - Dedicated: Check app_id suffix first as it's the most specific identifier
      - SM: Check app_id suffix or Self-Managed delivery type 
      - GitLab.com: Default for remaining SaaS instances or specific instance ID

      This order helps avoid misclassifying Dedicated instances as GitLab.com
      */
      CASE  
        WHEN app_id LIKE '%_dedicated' THEN 'Dedicated'
        WHEN app_id LIKE '%_sm' OR product_delivery_type = 'Self-Managed' THEN 'Self-Managed'
        WHEN product_delivery_type = 'SaaS' OR dim_instance_id = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f' THEN 'GitLab.com'
      END                                                                                                                                                            AS product_deployment_type,
      CASE
        WHEN LENGTH(unstructured_event) > 0 AND TRY_PARSE_JSON(unstructured_event) IS NULL
          THEN TRUE
        ELSE FALSE 
      END                                                                                                                                                            AS is_bad_unstructed_event,
      REGEXP_REPLACE(page_url, '^https?:\/\/')                                                                                                                       AS page_url_host_path,
      REGEXP_REPLACE(referrer_url, '^https?:\/\/')                                                                                                                  AS referrer_url_host_path,
     IFF(app_id IS NOT NULL
         AND derived_timestamp IS NOT NULL
         AND (
         (
           -- js backend tracker
           tracker_version LIKE 'js%'
           AND COALESCE(lower(page_url), '') NOT LIKE 'http://localhost:%'
         )
         OR
         (
           -- ruby backend tracker
           tracker_version LIKE 'rb%'
         )
         OR
         (
           -- code suggestions events
           tracker_version LIKE 'py%'
         )
         OR
         (
           -- jetbrains plugin events
           tracker_version LIKE 'java%'
         )
        )
        AND IFF(event_name IN ('submit_form', 'focus_form', 'change_form') AND derived_timestamp < '2021-05-26'
            , FALSE
            , TRUE)
        , TRUE
        , FALSE
        )                                                                                                                                                       AS is_good_snowplow_record,
      CASE
        WHEN app_id = 'gitlab-staging' THEN TRUE
        WHEN LOWER(page_url) LIKE 'https://staging.gitlab.com/%' THEN TRUE
        WHEN LOWER(page_url) LIKE 'https://customers.stg.gitlab.com/%' THEN TRUE
        ELSE FALSE
      END                                                                                                                                                       AS is_staging_event,
      {{dbt_utils.get_url_parameter(field='page_url_query', url_parameter='glm_source')}}                                                                        AS glm_source,

      -- cut flags
      IFF(
          event_name IN ('page_ping', 'page_view')
          AND session_id IS NOT NULL
          AND session_index IS NOT NULL
          AND user_snowplow_domain_id IS NOT NULL
          , TRUE
          , FALSE
          )::BOOLEAN                                                                                                                                           AS is_page_view_event

    FROM context_flattening

)

SELECT *
FROM derived_fields