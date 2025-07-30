{{ config({
    "materialized":"incremental",
    "incremental_strategy":"delete+insert",
    "unique_key":"page_view_id",
    "on_schema_change":"sync_all_columns",
    "snowflake_warehouse": generate_warehouse_name('XL')
  })
}}

WITH source AS (
  SELECT *
  FROM {{ ref('snowplow_unnested_events') }}
  WHERE event_name IN ('page_ping', 'page_view')
    AND domain_sessionid IS NOT NULL
    AND domain_sessionidx IS NOT NULL
    AND domain_userid IS NOT NULL
    {% if is_incremental() %}
      AND derived_tstamp > DATEADD('day', -7, (SELECT MAX(max_tstamp) FROM {{ this }}))
    {% endif %}
),

filtered_events AS (

  SELECT * FROM source
  {% if is_incremental() %}
    WHERE derived_tstamp > (SELECT MAX(max_tstamp) FROM {{ this }})
  {% endif %}



),

-- we need to grab all events for any session that has appeared
-- in order to correctly process the session index below
relevant_sessions AS (

  SELECT DISTINCT domain_sessionid
  FROM filtered_events
),

web_events AS (

  SELECT source.*
  FROM source
  INNER JOIN relevant_sessions ON source.domain_sessionid = relevant_sessions.domain_sessionid

),

page_view_aggrigation AS (

  SELECT

    web_page_id,
    ARRAY_AGG(IFF(
      event_name = 'page_view',
      OBJECT_CONSTRUCT(
        'domain_userid', domain_userid,
        'network_userid', network_userid,
        'domain_sessionid', domain_sessionid,
        'domain_sessionidx', domain_sessionidx,
        'page_urlhost', page_urlhost,
        'page_urlpath', page_urlpath,
        'page_urlscheme', page_urlscheme,
        'page_urlport', page_urlport,
        'page_urlquery', page_urlquery,
        'page_urlfragment', page_urlfragment,
        'page_title', page_title,
        'refr_urlscheme', refr_urlscheme,
        'refr_urlhost', refr_urlhost,
        'refr_urlport', refr_urlport,
        'refr_urlpath', refr_urlpath,
        'refr_urlquery', refr_urlquery,
        'refr_urlfragment', refr_urlfragment,
        'geo_country', geo_country,
        'geo_region', geo_region,
        'geo_region_name', geo_region_name,
        'geo_city', geo_city,
        'geo_timezone', geo_timezone,
        'app_id', app_id,
        'br_family', br_family,
        'br_name', br_name,
        'br_version', br_version,
        'os_family', os_family,
        'os_name', os_name,
        'br_lang', br_lang,
        'useragent', useragent,
        'br_type', br_type,
        'os_manufacturer', os_manufacturer,
        'os_timezone', (REPLACE(
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
            WHEN os_timezone = 'Asia/Singaporegpad8vnkdm' THEN 'Asia/Singapore'
            WHEN os_timezone = 'Asia/Singaporere6mvs9sb0' THEN 'Asia/Singapore'
            WHEN NOT REGEXP_LIKE(os_timezone,'^[A-Za-z0-9_+-]+\/[A-Za-z0-9_+-]+(\/[A-Za-z0-9_+-]+)*$') THEN NULL
            ELSE os_timezone
          END,
          '%2F', '/'
        )),
        'br_renderengine', br_renderengine,
        'dvce_type', dvce_type,
        'dvce_ismobile', dvce_ismobile,
        'glm_source', glm_source,
        'gsc_environment', gsc_environment,
        'gsc_extra', gsc_extra,
        'gsc_namespace_id', gsc_namespace_id,
        'gsc_plan', gsc_plan,
        'gsc_google_analytics_client_id', gsc_google_analytics_client_id,
        'gsc_project_id', gsc_project_id,
        'gsc_pseudonymized_user_id', gsc_pseudonymized_user_id,
        'gsc_source', gsc_source,
        'gsc_is_gitlab_team_member', gsc_is_gitlab_team_member,
        'page_referrer', page_referrer,
        'page_url', page_url,
        'dvce_created_tstamp', dvce_created_tstamp
      ),
      NULL
    ))                                                                       AS page_view_data,
    MIN(derived_tstamp::TIMESTAMP)                                           AS min_derived_at,
    MAX(derived_tstamp::TIMESTAMP)                                           AS max_derived_at,
    SUM(IFF(event_name = 'page_view', 1, 0))                                 AS page_view_count,
    SUM(IFF(event_name = 'page_ping', 1, 0))                                 AS page_ping_count,
    page_ping_count * 30                                                     AS seconds_engaged,

    MAX(IFF(doc_width = 0 OR doc_height = 0, NULL, doc_width))               AS max_document_width,
    MAX(IFF(doc_width = 0 OR doc_height = 0, NULL, doc_height))              AS max_document_height,
    MAX(IFF(doc_width = 0 OR doc_height = 0, NULL, br_viewwidth))            AS browser_view_width,
    MAX(IFF(doc_width = 0 OR doc_height = 0, NULL, br_viewheight))           AS browser_view_height,
    LEAST(GREATEST(MIN(COALESCE(pp_xoffset_min, 0)), 0), MAX(doc_width))     AS hmin,
    LEAST(GREATEST(MAX(COALESCE(pp_xoffset_max, 0)), 0), MAX(doc_width))     AS hmax,
    LEAST(GREATEST(MIN(COALESCE(pp_yoffset_min, 0)), 0), MAX(doc_height))    AS vmin,
    LEAST(GREATEST(MAX(COALESCE(pp_yoffset_max, 0)), 0), MAX(doc_height))    AS vmax,

    ROUND(100 * (GREATEST(hmin, 0) / NULLIF(max_document_width::FLOAT, 0)))  AS relative_hmin,
    ROUND(100 * (
      LEAST(hmax + browser_view_width, max_document_width)
      / NULLIF(max_document_width::FLOAT, 0)
    ))                                                                       AS relative_hmax,
    ROUND(100 * (GREATEST(vmin, 0) / NULLIF(max_document_height::FLOAT, 0))) AS relative_vmin,
    ROUND(100 * (
      LEAST(vmax + browser_view_height, max_document_height)
      / NULLIF(max_document_height::FLOAT, 0)
    ))                                                                       AS relative_vmax,

    -- Navigation timing metrics - collect raw values
    MIN(NULLIF(navigation_start, 0))                                         AS _navigation_start,
    MIN(NULLIF(redirect_start, 0))                                           AS _redirect_start,
    MIN(NULLIF(redirect_end, 0))                                             AS _redirect_end,
    MIN(NULLIF(fetch_start, 0))                                              AS _fetch_start,
    MIN(NULLIF(domain_lookup_start, 0))                                      AS _domain_lookup_start,
    MIN(NULLIF(domain_lookup_end, 0))                                        AS _domain_lookup_end,
    MIN(NULLIF(secure_connection_start, 0))                                  AS _secure_connection_start,
    MIN(NULLIF(connect_start, 0))                                            AS _connect_start,
    MIN(NULLIF(connect_end, 0))                                              AS _connect_end,
    MIN(NULLIF(request_start, 0))                                            AS _request_start,
    MIN(NULLIF(response_start, 0))                                           AS _response_start,
    MIN(NULLIF(response_end, 0))                                             AS _response_end,
    MIN(NULLIF(unload_event_start, 0))                                       AS _unload_event_start,
    MIN(NULLIF(unload_event_end, 0))                                         AS _unload_event_end,
    MIN(NULLIF(dom_loading, 0))                                              AS _dom_loading,
    MIN(NULLIF(dom_interactive, 0))                                          AS _dom_interactive,
    MIN(NULLIF(dom_content_loaded_event_start, 0))                           AS _dom_content_loaded_event_start,
    MIN(NULLIF(dom_content_loaded_event_end, 0))                             AS _dom_content_loaded_event_end,
    MIN(NULLIF(dom_complete, 0))                                             AS _dom_complete,
    MIN(NULLIF(load_event_start, 0))                                         AS _load_event_start,
    MIN(NULLIF(load_event_end, 0))                                           AS _load_event_end,

    -- Calculate time differences between navigation events
    -- Only calculate when both values exist and end time >= start time
    IFF(
      _redirect_start IS NOT NULL AND _redirect_end IS NOT NULL AND _redirect_end >= _redirect_start,
      _redirect_end - _redirect_start, NULL
    )                                                                        AS redirect_time_in_ms,

    IFF(
      _unload_event_start IS NOT NULL AND _unload_event_end IS NOT NULL AND _unload_event_end >= _unload_event_start,
      _unload_event_end - _unload_event_start, NULL
    )                                                                        AS unload_time_in_ms,

    IFF(
      _fetch_start IS NOT NULL AND _domain_lookup_start IS NOT NULL AND _domain_lookup_start >= _fetch_start,
      _domain_lookup_start - _fetch_start, NULL
    )                                                                        AS app_cache_time_in_ms,

    IFF(
      _domain_lookup_start IS NOT NULL AND _domain_lookup_end IS NOT NULL AND _domain_lookup_end >= _domain_lookup_start,
      _domain_lookup_end - _domain_lookup_start, NULL
    )                                                                        AS dns_time_in_ms,

    IFF(
      _connect_start IS NOT NULL AND _connect_end IS NOT NULL AND _connect_end >= _connect_start,
      _connect_end - _connect_start, NULL
    )                                                                        AS tcp_time_in_ms,

    IFF(
      _request_start IS NOT NULL AND _response_start IS NOT NULL AND _response_start >= _request_start,
      _response_start - _request_start, NULL
    )                                                                        AS request_time_in_ms,

    IFF(
      _response_start IS NOT NULL AND _response_end IS NOT NULL AND _response_end >= _response_start,
      _response_end - _response_start, NULL
    )                                                                        AS response_time_in_ms,

    IFF(
      _dom_loading IS NOT NULL AND _dom_complete IS NOT NULL AND _dom_complete >= _dom_loading,
      _dom_complete - _dom_loading, NULL
    )                                                                        AS processing_time_in_ms,

    IFF(
      _dom_loading IS NOT NULL AND _dom_interactive IS NOT NULL AND _dom_interactive >= _dom_loading,
      _dom_interactive - _dom_loading, NULL
    )                                                                        AS dom_loading_to_interactive_time_in_ms,

    IFF(
      _dom_interactive IS NOT NULL AND _dom_complete IS NOT NULL AND _dom_complete >= _dom_interactive,
      _dom_complete - _dom_interactive, NULL
    )                                                                        AS dom_interactive_to_complete_time_in_ms,

    IFF(
      _load_event_start IS NOT NULL AND _load_event_end IS NOT NULL AND _load_event_end >= _load_event_start,
      _load_event_end - _load_event_start, NULL
    )                                                                        AS onload_time_in_ms,

    IFF(
      _navigation_start IS NOT NULL AND _load_event_end IS NOT NULL AND _load_event_end >= _navigation_start,
      _load_event_end - _navigation_start, NULL
    )                                                                        AS total_time_in_ms

  FROM web_events
  GROUP BY 1
  HAVING page_view_count > 0
    AND max_document_width > 0
    AND max_document_height > 0

),

session_index AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY page_view_data[0]['domain_userid'] ORDER BY page_view_data[0]['dvce_created_tstamp'])    AS page_view_index,
    ROW_NUMBER() OVER (PARTITION BY page_view_data[0]['domain_sessionid'] ORDER BY page_view_data[0]['dvce_created_tstamp']) AS page_view_in_session_index,
    COUNT(*) OVER (PARTITION BY page_view_data[0]['domain_sessionid'])                                                       AS max_session_page_view_index,

    IFF(max_session_page_view_index = page_view_in_session_index, TRUE, FALSE)                                               AS is_last_page_view_in_session,

    -- No documentation for why this is being converted, recomend not converting in future revision
    CONVERT_TIMEZONE('UTC', 'America/New_York', min_derived_at)                                                              AS page_view_start,
    CONVERT_TIMEZONE('UTC', 'America/New_York', max_derived_at)                                                              AS page_view_end,

    COALESCE(page_view_data[0]['os_timezone'], 'America/New_York')                                                           AS page_view_start_local,
    COALESCE(page_view_data[0]['os_timezone'], 'America/New_York')                                                           AS page_view_end_local,

    CASE
      WHEN seconds_engaged BETWEEN 0 AND 9 THEN '0s to 9s'
      WHEN seconds_engaged BETWEEN 10 AND 29 THEN '10s to 29s'
      WHEN seconds_engaged BETWEEN 30 AND 59 THEN '30s to 59s'
      WHEN seconds_engaged > 59 THEN '60s or more'
    END                                                                                                                      AS seconds_engaged_tier,

    hmax                                                                                                                     AS horizontal_pixels_scrolled,
    vmax                                                                                                                     AS vertical_pixels_scrolled,

    relative_hmax                                                                                                            AS horizontal_percentage_scrolled,
    relative_vmax                                                                                                            AS vertical_percentage_scrolled,

    CASE
      WHEN relative_vmax BETWEEN 0 AND 24 THEN '0% to 24%'
      WHEN relative_vmax BETWEEN 25 AND 49 THEN '25% to 49%'
      WHEN relative_vmax BETWEEN 50 AND 74 THEN '50% to 74%'
      WHEN relative_vmax BETWEEN 75 AND 100 THEN '75% to 100%'
    END                                                                                                                      AS vertical_percentage_scrolled_tier,

    IFF(seconds_engaged >= 30 AND relative_vmax >= 25, TRUE, FALSE)                                                          AS was_user_engaged

  FROM page_view_aggrigation
  -- Bot filter logic can be moved upstream
  WHERE (page_view_data[0]['br_family'] != 'Robot/Spider' OR page_view_data[0]['br_family'] IS NULL)
    AND (
      NOT (
        LOWER(page_view_data[0]['useragent']) LIKE '%bot%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%crawl%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%slurp%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%spider%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%archiv%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%spinn%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%sniff%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%seo%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%audit%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%survey%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%pingdom%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%worm%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%capture%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%browsershots%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%screenshots%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%analyz%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%index%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%thumb%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%check%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%facebook%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%phantomjs%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%a_archiver%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%facebookexternalhit%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%bingpreview%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%360user-agent%'
        OR LOWER(page_view_data[0]['useragent']) LIKE '%semalt%'
      )
      OR page_view_data[0]['useragent'] IS NULL
    )
    AND COALESCE(page_view_data[0]['br_type'], 'unknown') NOT IN ('Bot/Crawler', 'Robot')
    AND page_view_data[0]['domain_userid'] IS NOT NULL
    AND page_view_data[0]['domain_sessionidx']::INT > 0

)

SELECT
  page_view_data[0]['domain_userid']::VARCHAR                  AS user_snowplow_domain_id,
  page_view_data[0]['network_userid']::VARCHAR                 AS user_snowplow_crossdomain_id,
  min_derived_at                                               AS min_tstamp,
  max_derived_at                                               AS max_tstamp,
  page_view_data[0]['domain_sessionid']::VARCHAR               AS session_id,
  page_view_data[0]['domain_sessionidx']::VARCHAR              AS session_index,
  web_page_id                                                  AS page_view_id,
  page_view_index,
  page_view_in_session_index,
  max_session_page_view_index,
  page_view_start,
  page_view_end,
  page_view_start_local,
  page_view_end_local,
  seconds_engaged                                              AS time_engaged_in_s,
  seconds_engaged_tier                                         AS time_engaged_in_s_tier,
  horizontal_pixels_scrolled,
  vertical_pixels_scrolled,
  horizontal_percentage_scrolled,
  vertical_percentage_scrolled,
  vertical_percentage_scrolled_tier,
  was_user_engaged                                             AS user_engaged,
  page_view_data[0]['page_urlscheme']::VARCHAR                 AS page_url_scheme,
  page_view_data[0]['page_urlhost']::VARCHAR                   AS page_url_host,
  page_view_data[0]['page_urlport']::VARCHAR                   AS page_url_port,
  page_view_data[0]['page_urlpath']::VARCHAR                   AS page_url_path,
  page_view_data[0]['page_urlquery']::VARCHAR                  AS page_url_query,
  page_view_data[0]['page_urlfragment']::VARCHAR               AS page_url_fragment,
  page_view_data[0]['page_title']::VARCHAR                     AS page_title,
  page_url_host || page_url_path                               AS page_url,
  max_document_width                                           AS page_width,
  max_document_height                                          AS page_height,
  page_view_data[0]['refr_urlscheme']::VARCHAR                 AS referer_url_scheme,
  page_view_data[0]['refr_urlhost']::VARCHAR                   AS referer_url_host,
  page_view_data[0]['refr_urlport']::VARCHAR                   AS referer_url_port,
  page_view_data[0]['refr_urlpath']::VARCHAR                   AS referer_url_path,
  page_view_data[0]['refr_urlquery']::VARCHAR                  AS referer_url_query,
  page_view_data[0]['refr_urlfragment']::VARCHAR               AS referer_url_fragment,
  referer_url_host || referer_url_path                         AS referer_url,
  page_view_data[0]['geo_country']::VARCHAR                    AS geo_country,
  page_view_data[0]['geo_region']::VARCHAR                     AS geo_region,
  page_view_data[0]['geo_region_name']::VARCHAR                AS geo_region_name,
  page_view_data[0]['geo_city']::VARCHAR                       AS geo_city,
  NULL                                                         AS geo_zipcode,
  NULL                                                         AS geo_latitude,
  NULL                                                         AS geo_longitude,
  page_view_data[0]['geo_timezone']::VARCHAR                   AS geo_timezone,
  NULL                                                         AS ip_address,
  page_view_data[0]['app_id']::VARCHAR                         AS app_id,
  NULL                                                         AS browser,
  page_view_data[0]['br_family']::VARCHAR                      AS browser_name,
  page_view_data[0]['br_name']::VARCHAR                        AS browser_major_version,
  page_view_data[0]['br_version']::VARCHAR                     AS browser_minor_version,
  NULL                                                         AS browser_build_version,
  page_view_data[0]['os_family']::VARCHAR                      AS os,
  page_view_data[0]['os_name']::VARCHAR                        AS os_name,
  NULL                                                         AS os_major_version,
  NULL                                                         AS os_minor_version,
  NULL                                                         AS os_build_version,
  NULL                                                         AS device,
  browser_view_width                                           AS browser_window_width,
  browser_view_height                                          AS browser_window_height,
  page_view_data[0]['br_lang']::VARCHAR                        AS browser_language,
  page_view_data[0]['os_manufacturer']::VARCHAR                AS os_manufacturer,
  page_view_data[0]['os_timezone']::VARCHAR                    AS os_timezone,

  -- in previsou set up timing was only comming from struct events, 
  redirect_time_in_ms,
  unload_time_in_ms,
  app_cache_time_in_ms,
  dns_time_in_ms,
  tcp_time_in_ms,
  request_time_in_ms,
  response_time_in_ms,
  processing_time_in_ms,
  dom_loading_to_interactive_time_in_ms,
  dom_interactive_to_complete_time_in_ms,
  onload_time_in_ms,
  total_time_in_ms,

  page_view_data[0]['br_renderengine']::VARCHAR                AS browser_engine,
  page_view_data[0]['dvce_type']::VARCHAR                      AS device_type,
  page_view_data[0]['dvce_ismobile']::VARCHAR                  AS device_is_mobile,
  page_view_data[0]['glm_source']::VARCHAR                     AS glm_source,
  page_view_data[0]['gsc_environment']::VARCHAR                AS gsc_environment,
  page_view_data[0]['gsc_extra']                               AS gsc_extra,
  page_view_data[0]['gsc_namespace_id']::INT                   AS gsc_namespace_id,
  page_view_data[0]['gsc_plan']::VARCHAR                       AS gsc_plan,
  page_view_data[0]['gsc_google_analytics_client_id']::VARCHAR AS gsc_google_analytics_client_id,
  page_view_data[0]['gsc_project_id']::INT                     AS gsc_project_id,
  page_view_data[0]['gsc_pseudonymized_user_id']::VARCHAR      AS gsc_pseudonymized_user_id,
  page_view_data[0]['gsc_source']::VARCHAR                     AS gsc_source,
  page_view_data[0]['gsc_is_gitlab_team_member']::VARCHAR      AS gsc_is_gitlab_team_member,
  NULL                                                         AS cf_formid,
  NULL                                                         AS cf_elementid,
  NULL                                                         AS cf_nodename,
  NULL                                                         AS cf_type,
  NULL                                                         AS cf_elementclasses,
  NULL                                                         AS cf_value,
  NULL                                                         AS sf_formid,
  NULL                                                         AS sf_formclasses,
  NULL                                                         AS sf_elements,
  NULL                                                         AS ff_formid,
  NULL                                                         AS ff_elementid,
  NULL                                                         AS ff_nodename,
  NULL                                                         AS ff_elementtype,
  NULL                                                         AS ff_elementclasses,
  NULL                                                         AS ff_value,
  NULL                                                         AS lc_elementid,
  NULL                                                         AS lc_elementclasses,
  NULL                                                         AS lc_elementtarget,
  NULL                                                         AS lc_targeturl,
  NULL                                                         AS lc_elementcontent,
  NULL                                                         AS tt_category,
  NULL                                                         AS tt_variable,
  NULL                                                         AS tt_timing,
  NULL                                                         AS tt_label,
  page_view_data[0]['page_referrer']::VARCHAR                  AS page_referrer,
  page_view_data[0]['page_url']::VARCHAR                       AS page_url_original,
  IFF(is_last_page_view_in_session,1,0)                        AS last_page_view_in_session
FROM session_index
