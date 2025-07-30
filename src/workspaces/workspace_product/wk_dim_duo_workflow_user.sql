{{ config(
    materialized = 'table',
    tags = ["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('mart_behavior_structured_event', 'mart_behavior_structured_event')
    ])
}},

/*
    This dimension model represents the relationship between users, namespaces, and workflows.
    Following Kimball dimensional modeling principles, this serves as a bridge table
    connecting users to workflows, with the namespace as additional context.
*/

final AS (
    
SELECT DISTINCT
    event_value AS workflow_id,
    ultimate_parent_namespace_id,
    gitlab_global_user_id
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

)

SELECT *
FROM final