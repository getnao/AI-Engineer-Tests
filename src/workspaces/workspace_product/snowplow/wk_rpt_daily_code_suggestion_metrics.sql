{{ config(
    materialized="table",
    tags=["product", "mnpi_exception"]
) }}

WITH code_suggestion_events AS (
  SELECT 
    behavior_structured_event_pk,
    behavior_at,
    behavior_at::DATE AS behavior_date,
    event_action, 
    enabled_by_dim_crm_account_id_at_event_time,
    gitlab_global_user_id
  FROM {{ ref('mart_behavior_structured_event_ai_gateway_flattened') }}
  WHERE feature_category = 'code_suggestions'
    AND app_id = 'gitlab_ai_gateway'
),

all_events_pre_aggregated AS (
  SELECT
    behavior_at::DATE                 AS behavior_date,
    DATE_TRUNC('second', behavior_at) AS event_second,
    COUNT(*)                          AS events_count
  FROM {{ ref('mart_behavior_structured_event') }}
  GROUP BY 1, 2
),

daily_all_events AS (
  SELECT
    behavior_date,
    SUM(events_count) AS total_all_events
  FROM all_events_pre_aggregated
  GROUP BY 1
),

daily_peak_events_all_events AS (
  SELECT
    behavior_date,
    MAX(events_count) AS peak_all_events_per_second
  FROM all_events_pre_aggregated
  GROUP BY 1
),

events_per_second_code_suggestions AS (
  SELECT
    behavior_date,
    DATE_TRUNC('second', behavior_at)             AS event_second,
    COUNT(DISTINCT behavior_structured_event_pk)  AS events_per_second
  FROM code_suggestion_events
  GROUP BY 1, 2
),

daily_peak_events_code_suggestions AS (
  SELECT
    behavior_date,
    MAX(events_per_second) AS peak_events_per_second
  FROM events_per_second_code_suggestions
  GROUP BY 1
),

events_per_second_by_action AS (
  SELECT
    behavior_date,
    event_action,
    DATE_TRUNC('second', behavior_at)               AS event_second,
    COUNT(DISTINCT behavior_structured_event_pk)    AS events_per_second
  FROM code_suggestion_events
  WHERE event_action IN ('request_generate_code', 'request_complete_code')
  GROUP BY 1, 2, 3
),

daily_peak_events_by_action AS (
  SELECT
    behavior_date,
    event_action,
    MAX(events_per_second) AS peak_events_per_second
  FROM events_per_second_by_action
  GROUP BY 1, 2
),

peak_generate_code_events AS (
  SELECT
    behavior_date,
    peak_events_per_second AS peak_generate_code_events_per_second
  FROM daily_peak_events_by_action
  WHERE event_action = 'request_generate_code'
),

peak_complete_code_events AS (
  SELECT
    behavior_date,
    peak_events_per_second AS peak_complete_code_events_per_second
  FROM daily_peak_events_by_action
  WHERE event_action = 'request_complete_code'
),

daily_code_suggestion_events AS (
  SELECT
    behavior_date,
    COUNT(DISTINCT behavior_structured_event_pk)                AS total_code_suggestion_events, 
    COUNT(DISTINCT enabled_by_dim_crm_account_id_at_event_time) AS total_active_accounts,
    COUNT(DISTINCT gitlab_global_user_id)                       AS total_active_users 
  FROM code_suggestion_events
  GROUP BY 1
),

daily_events_by_action AS (
  SELECT
    behavior_date,
    event_action,
    COUNT(DISTINCT behavior_structured_event_pk) AS action_events
  FROM code_suggestion_events
  WHERE event_action IN ('request_generate_code', 'request_complete_code')
  GROUP BY 1, 2
),

request_generate_code_metrics AS (
  SELECT
    behavior_date,
    action_events AS request_generate_code_events
  FROM daily_events_by_action
  WHERE event_action = 'request_generate_code'
),

request_complete_code_metrics AS (
  SELECT
    behavior_date,
    action_events AS request_complete_code_events
  FROM daily_events_by_action
  WHERE event_action = 'request_complete_code'
)

SELECT
  daily_all_events.behavior_date,
  daily_all_events.total_all_events,
  COALESCE(daily_peak_events_all_events.peak_all_events_per_second, 0)        AS peak_all_events_per_second,
  COALESCE(daily_code_suggestion_events.total_code_suggestion_events, 0)      AS total_code_suggestion_events,
  COALESCE(daily_peak_events_code_suggestions.peak_events_per_second, 0)      AS peak_code_suggestion_events_per_second,
  COALESCE(daily_code_suggestion_events.total_active_accounts, 0)             AS total_active_code_suggestion_accounts,
  COALESCE(daily_code_suggestion_events.total_active_users, 0)                AS total_active_code_suggestion_users,
  COALESCE(request_generate_code_metrics.request_generate_code_events, 0)     AS request_generate_code_events,
  COALESCE(peak_generate_code_events.peak_generate_code_events_per_second, 0) AS peak_generate_code_events_per_second,
  COALESCE(request_complete_code_metrics.request_complete_code_events, 0)     AS request_complete_code_events,
  COALESCE(peak_complete_code_events.peak_complete_code_events_per_second, 0) AS peak_complete_code_events_per_second
FROM daily_all_events
LEFT JOIN daily_peak_events_all_events
  ON daily_all_events.behavior_date = daily_peak_events_all_events.behavior_date
LEFT JOIN daily_code_suggestion_events
  ON daily_all_events.behavior_date = daily_code_suggestion_events.behavior_date
LEFT JOIN daily_peak_events_code_suggestions
  ON daily_all_events.behavior_date = daily_peak_events_code_suggestions.behavior_date
LEFT JOIN request_generate_code_metrics
  ON daily_all_events.behavior_date = request_generate_code_metrics.behavior_date
LEFT JOIN request_complete_code_metrics
  ON daily_all_events.behavior_date = request_complete_code_metrics.behavior_date
LEFT JOIN peak_generate_code_events
  ON daily_all_events.behavior_date = peak_generate_code_events.behavior_date
LEFT JOIN peak_complete_code_events
  ON daily_all_events.behavior_date = peak_complete_code_events.behavior_date
