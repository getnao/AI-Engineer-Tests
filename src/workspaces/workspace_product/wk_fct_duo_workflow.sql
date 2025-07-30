{{ config(
    materialized = 'table',
    tags = ["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('mart_behavior_structured_event', 'mart_behavior_structured_event'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_crm_account', 'dim_crm_account'),
    ('mart_arr_all_weekly', 'mart_arr_weekly_with_zero_dollar_charges')
    ])
}},

/*
    This fact model captures comprehensive workflow metrics and statuses.
    It follows Kimball dimensional modeling principles by focusing on
    measurable workflow events and linking to appropriate dimensions.
    Each row represents a single workflow with its metrics and attributes.
*/

duo_workflow_events AS (

  SELECT 
    -- Timestamp and identification columns
    behavior_at,
    behavior_date,
    ultimate_parent_namespace_id,
    gitlab_global_user_id,
    behavior_structured_event_pk,
    dim_instance_id,
    gsc_correlation_id,
    -- Event classification columns
    event_action,
    event_category,
    event_property,
    event_value AS workflow_id,
    -- User/namespace metadata
    gsc_is_gitlab_team_member,  
    namespace_is_internal,
    -- Derived fields for workflow analysis
    IFF(event_action = 'request_duo_workflow', event_category, NULL) 
      AS duo_workflow_request_category,
    IFF(event_property = 'cancelled_by_user', TRUE, FALSE) 
      AS workflow_cancelled_by_user,
    -- Token usage metrics
    PARSE_JSON(contexts):data[0]:data:extra:input_tokens AS input_tokens,
    PARSE_JSON(contexts):data[0]:data:extra:output_tokens AS output_tokens
  FROM mart_behavior_structured_event
  WHERE 
     event_action IN (
        'request_duo_workflow', 
        'request_duo_workflow_success', 
        'request_duo_workflow_failure', 
        'duo_workflow_tool_failure', 
        'resume_duo_workflow',
        'tokens_per_user_request_prompt'
    )
    AND app_id = 'gitlab_duo_workflow'

), 

workflows_and_accounts_prep AS (

  SELECT DISTINCT 
    a.dim_crm_account_id,
    a.dim_parent_crm_account_id,
    a.crm_account_name,
    a.parent_crm_account_name,
    e.workflow_id,
    e.ultimate_parent_namespace_id,    
    ZEROIFNULL(SUM(arr.quantity)) AS seats
  FROM duo_workflow_events AS e
  INNER JOIN dim_subscription AS subscription
    ON subscription.namespace_id = e.ultimate_parent_namespace_id
    AND e.behavior_date BETWEEN subscription.term_start_date 
      AND subscription.term_end_date
  INNER JOIN dim_crm_account AS a
    ON a.dim_crm_account_id = subscription.dim_crm_account_id
  LEFT JOIN mart_arr_all_weekly AS arr
    ON arr.dim_crm_account_id = subscription.dim_crm_account_id
    AND arr.arr_week = DATE_TRUNC('week', e.behavior_date)
  GROUP BY 
    a.dim_crm_account_id,
    a.dim_parent_crm_account_id,
    a.crm_account_name,
    a.parent_crm_account_name,
    e.workflow_id,
    e.ultimate_parent_namespace_id

), 

/*
Choose the account with the highest seat count when multiple accounts
are associated with the same namespace+workflow.
Example as of 2025-04-07: crm_account_name Thomas Bastian and 
CompareTheMarket - both associated with top level namespace 12489339
*/
account_workflow AS (

  SELECT 
    *
  FROM workflows_and_accounts_prep
  QUALIFY ROW_NUMBER() OVER(PARTITION BY workflow_id ORDER BY seats DESC) = 1

), 

first_actions AS ( -- first event in the duo workflow 
/* Only workflows starting with a request_duo_workflow event are
 included in the final result as valid workflows.
 An uncommon edgecase exists in the data where a unique workflow_id 
 starts with a request_duo_workflow_failure event. This scenario is not a valid workflow. 
*/

  SELECT 
    workflow_id,
    event_action AS first_action,
    behavior_at AS first_timestamp,
    ultimate_parent_namespace_id,
    IFF(duo_workflow_request_category IN ('GitLabWorkflow', 'Workflow'), 'software_development', duo_workflow_request_category)
          AS duo_workflow_request_category
  FROM duo_workflow_events
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workflow_id ORDER BY behavior_at ASC) = 1

), 

last_actions AS ( -- last event in the duo workflow

  SELECT 
    workflow_id,
    event_action AS last_action,
    behavior_at AS last_timestamp
  FROM duo_workflow_events
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workflow_id ORDER BY behavior_at DESC) = 1

), 

tokens AS (

  SELECT 
    workflow_id,
    MIN(behavior_date) AS behavior_date,  -- Start date of the workflow
    SUM(input_tokens) AS input_tokens,
    SUM(output_tokens) AS output_tokens,
    SUM(input_tokens + output_tokens) AS overall_tokens
  FROM duo_workflow_events
  WHERE event_action = 'tokens_per_user_request_prompt'
  GROUP BY 
    workflow_id

), 

workflow_actions AS ( -- Grouping metrics for workflows

  SELECT 
    m.workflow_id,
    f.ultimate_parent_namespace_id, --namespace identifier associated with first workflow request event
    MIN(m.behavior_date) AS workflow_start_date,  -- Start date of the workflow
    f.first_timestamp,
    l.last_timestamp,
    f.first_action,
    f.duo_workflow_request_category, --event_category associated with first workflow request event
    COUNT(DISTINCT m.ultimate_parent_namespace_id) AS count_namespaces,
    ARRAY_AGG(DISTINCT m.ultimate_parent_namespace_id) 
      WITHIN GROUP (ORDER BY m.ultimate_parent_namespace_id) 
      AS ultimate_parent_namespace_ids, -- array including all top level namespace ids associated with all events sharing a workflow_id
    COUNT(DISTINCT gitlab_global_user_id) AS count_users,
    MAX(m.workflow_cancelled_by_user) AS workflow_cancelled_by_user,
    MAX(
      IFF(m.event_action = 'resume_duo_workflow', TRUE, FALSE)
    ) AS is_resumed,
    -- All event actions for status determination
    LISTAGG(DISTINCT m.event_action, ', ') 
      WITHIN GROUP (ORDER BY m.event_action) AS event_action_array,      
    MAX(m.namespace_is_internal) AS namespace_is_internal,
    MAX(m.gsc_is_gitlab_team_member) AS gsc_is_gitlab_team_member,
    CASE WHEN MAX(m.namespace_is_internal) = TRUE OR MAX(m.gsc_is_gitlab_team_member) = TRUE THEN TRUE 
      WHEN MAX(m.namespace_is_internal) = FALSE OR MAX(m.gsc_is_gitlab_team_member) = FALSE THEN FALSE END 
      AS is_internal_usage_any
  FROM duo_workflow_events AS m
  INNER JOIN first_actions AS f 
    ON m.workflow_id = f.workflow_id
  INNER JOIN last_actions AS l 
    ON m.workflow_id = l.workflow_id
  GROUP BY 
    m.workflow_id,
    f.ultimate_parent_namespace_id,
    f.first_action,
    f.first_timestamp,
    f.duo_workflow_request_category,
    l.last_timestamp

), 

final AS (

  SELECT DISTINCT
    -- Workflow identification and timing
    w.workflow_id,
    w.workflow_start_date,
    w.first_timestamp,
    w.last_timestamp,
    ROUND(
      DATEDIFF(SECOND, w.first_timestamp, w.last_timestamp) / 60.0, 
      2
    ) AS workflow_duration_minutes,
    -- Namespace/user information
    w.ultimate_parent_namespace_id,  -- From first request
    w.ultimate_parent_namespace_ids, -- All namespaces where events occurred
    w.namespace_is_internal,
    w.count_namespaces,
    w.gsc_is_gitlab_team_member,
    w.is_internal_usage_any,
    -- Account information
    a.dim_crm_account_id,
    a.dim_parent_crm_account_id,
    a.crm_account_name,
    a.parent_crm_account_name,
    -- User information
    w.count_users,
    -- Workflow metadata
    w.duo_workflow_request_category,
    w.event_action_array,
    -- Workflow status flags
    w.workflow_cancelled_by_user AS is_cancelled,
    w.is_resumed,
    -- Workflow status classification
    CASE 
      WHEN w.event_action_array LIKE '%duo_workflow_tool_failure%' 
        THEN 'Tool Fail'
      WHEN w.event_action_array LIKE '%request_duo_workflow_success%' 
        AND w.workflow_cancelled_by_user = TRUE 
        THEN 'Stopped'
      WHEN w.event_action_array LIKE '%request_duo_workflow_success%' 
        THEN 'Complete'
      WHEN w.event_action_array LIKE '%request_duo_workflow_failure%' 
        THEN 'Failure'
      ELSE 'Incomplete' 
    END AS workflow_status,
    -- High-level finished status
    CASE 
      WHEN workflow_status = 'Stopped'
        THEN 'Unfinished'
      WHEN workflow_status = 'Incomplete' 
        THEN 'Unfinished'
      ELSE 'Finished' 
    END AS finished_status,
    -- Token usage metrics
    t.input_tokens,
    t.output_tokens,
    t.overall_tokens
  FROM workflow_actions AS w
  LEFT JOIN tokens AS t
    ON t.workflow_id = w.workflow_id
  LEFT JOIN account_workflow AS a
    ON a.workflow_id = w.workflow_id
  WHERE w.first_action = 'request_duo_workflow'  -- Filter out invalid workflows

)

SELECT *
FROM final