{{ simple_cte([
    ('source', 'prep_support_ticket')
]) }},

final AS (

  SELECT 
    -- Primary identifiers
    {{ dbt_utils.generate_surrogate_key([
      'dim_support_ticket_id', 
      'updated_at'
    ]) }}                                 AS fct_support_ticket_sk,
    dim_support_ticket_id,
    external_id,

    -- Foreign keys
    assignee_id,
    brand_id,
    forum_topic_id,
    dim_support_organization_id,
    requester_id,
    submitter_id,
    group_id,
    custom_status_id,
    problem_id,
    dim_support_ticket_form_id,
    merged_ticket_ids,
    followup_ids,

    -- Ticket attributes
    has_incidents,
    allow_channelback,
    allow_attachments,
    ticket_url,

    -- GitLab custom fields 
    ticket_weight,
    ces_score,

    -- Timestamps
    created_at,
    updated_at,
    due_at,
    solved_at,

    -- Flags
    is_public,
    is_assignee_ooo,
    is_duplicate,
    is_follow_up,
    is_2fa,
    is_engineering_involved,
    is_closed,
    is_reduced_effort,
    has_plan,

    -- Resolution information
    time_to_resolve_minutes,
    time_to_resolve_hours,
    
    -- First Reply Time
    first_reply_time_minutes,
    first_reply_time_hours,
    
    -- Wait times
    requester_wait_time_minutes,
    requester_wait_time_hours,
    customer_wait_time_minutes,
    customer_wait_time_hours,
    customer_wait_time_ratio


  FROM source

)

SELECT * 
FROM final