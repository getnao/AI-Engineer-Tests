{{ simple_cte([
    ('ticket_source', 'zendesk_fivetran_ticket_source'),
    ('ticket_tags_source', 'zendesk_fivetran_ticket_tag_source'),
    ('ticket_field_history', 'prep_support_ticket_field_history'),
    ('ticket_comments', 'prep_support_ticket_comment'),
    ('user_source', 'zendesk_fivetran_user_source'),
]) }},

ticket_tags AS (
    SELECT 
        ticket_id AS dim_support_ticket_id,
        -- Create a list of the tags associated with a given ticket
        COALESCE(
            LISTAGG(lower(tag_name), ', ') WITHIN GROUP (ORDER BY tag_name), 
            ''
        ) AS ticket_tags,
        -- Also create array for precise tag matching
        ARRAY_AGG(tag_name) AS ticket_tags_array
    FROM ticket_tags_source
    GROUP BY 1
),

-- Enhanced solved_tickets CTE to calculate resolution time
solved_tickets AS (
    SELECT 
        dim_support_ticket_id,
        MAX(updated_at) AS solved_at  -- Use MAX instead of MIN to get the latest solved timestamp
    FROM ticket_field_history
    WHERE field_name = 'status' 
      AND field_value = 'solved'
    GROUP BY 1
),

-- Calculate wait times: RWT and CWT
ticket_status_transitions AS (
    -- Get all status transitions with their timestamps
    SELECT 
        dim_support_ticket_id,
        field_value AS status,
        updated_at AS status_start_time,
        -- Get the next status change time using LEAD function
        LEAD(updated_at) OVER (
            PARTITION BY dim_support_ticket_id 
            ORDER BY updated_at
        ) AS status_end_time
    FROM ticket_field_history 
    WHERE field_name = 'status'
        AND field_value IS NOT NULL
),

status_durations AS (
  -- Calculate time spent in each status
  SELECT 
      ticket_status_transitions.dim_support_ticket_id,
      ticket_status_transitions.status,
      -- Use solved_at as end time if no next status change (for final status)
      COALESCE(
        ticket_status_transitions.status_end_time, 
        solved_tickets.solved_at, 
        CURRENT_TIMESTAMP() -- For tickets still open
      ) AS status_end_time,

      -- Calculate duration in minutes
      DATEDIFF('minute', 
          ticket_status_transitions.status_start_time, 
          status_end_time
      ) AS status_duration_minutes
  FROM ticket_status_transitions 
  LEFT JOIN solved_tickets 
    ON ticket_status_transitions.dim_support_ticket_id = solved_tickets.dim_support_ticket_id
),

ticket_wait_times AS (
  -- Calculate RWT and CWT for each ticket
  SELECT 
      dim_support_ticket_id,
      
      -- Requester Wait Time (RWT)
      SUM(CASE 
          WHEN LOWER(status) IN ('new', 'open', 'hold') 
          THEN status_duration_minutes 
          ELSE 0 
      END) AS requester_wait_time_minutes,
      
      -- Customer Wait Time (CWT): Time in New and Open statuses only
      SUM(CASE 
          WHEN LOWER(status) IN ('new', 'open') 
          THEN status_duration_minutes 
          ELSE 0 
      END) AS customer_wait_time_minutes
      
  FROM status_durations
  GROUP BY dim_support_ticket_id

),

-- Calculate time to resolve with business logic adjustments
ticket_resolution_time AS (

    SELECT 
        ticket_source.ticket_id AS dim_support_ticket_id,
        ticket_source.created_at AS ticket_created_at,
        solved_tickets.solved_at,
        
        -- Apply GitLab business rules for MTTR calculation (based on autosolve tags)
        CASE 
            -- skip_autosolve: no adjustment
            WHEN ARRAYS_OVERLAP(ticket_tags.ticket_tags_array, ['skip_autosolve'::VARIANT])
                THEN DATEDIFF('minute', ticket_source.created_at, solved_tickets.solved_at)
                
            -- autosolve_ticket: subtract 168 hours (7 days), minimum 0
            WHEN ARRAYS_OVERLAP(ticket_tags.ticket_tags_array, ['autosolve_ticket'::VARIANT])
                THEN GREATEST(0, DATEDIFF('minute', ticket_source.created_at, solved_tickets.solved_at) - (168 * 60))
                
            -- pending_followup_notification + ticket_autosolve: subtract 336 hours (14 days), minimum 0
            WHEN ARRAYS_OVERLAP(ticket_tags.ticket_tags_array, ['pending_followup_notification'::VARIANT])
                AND ARRAYS_OVERLAP(ticket_tags.ticket_tags_array, ['ticket_autosolve'::VARIANT])
                THEN GREATEST(0, DATEDIFF('minute', ticket_source.created_at, solved_tickets.solved_at) - (336 * 60))
                
            -- autosolve: subtract 480 hours (20 days), minimum 0
            WHEN ARRAYS_OVERLAP(ticket_tags.ticket_tags_array, ['autosolve'::VARIANT])
                THEN GREATEST(0, DATEDIFF('minute', ticket_source.created_at, solved_tickets.solved_at) - (480 * 60))
                
            -- No adjustment
            ELSE DATEDIFF('minute', ticket_source.created_at, solved_tickets.solved_at)
        END AS time_to_resolve_minutes
    FROM ticket_source
    INNER JOIN solved_tickets 
        ON ticket_source.ticket_id = solved_tickets.dim_support_ticket_id
    LEFT JOIN ticket_tags
        ON ticket_source.ticket_id = ticket_tags.dim_support_ticket_id
),

-- Improved First Reply Time Calculation
ticket_comments_with_roles AS (
    SELECT 
        ticket_comments.dim_support_ticket_comment_id     AS comment_id,
        ticket_comments.dim_support_ticket_id             AS ticket_id,
        ticket_source.created_at                          AS ticket_created_at,
        ticket_comments.created_at                        AS comment_created_at,
        ticket_comments.is_public,
        
        -- Improved agent classification logic
        CASE 
            -- All GitLab Support Organization users are considered agents
            -- This includes all known support roles: 1288263, 8869919308956, 360004957599, 360001716320
            WHEN user_source.organization_id = 28655938 THEN 'gitlab_agent'
            
            -- Ticket-specific roles (fallback for edge cases)
            WHEN ticket_comments.dim_support_user_id = ticket_source.assignee_id THEN 'assigned_agent'
            WHEN ticket_comments.dim_support_user_id = ticket_source.requester_id THEN 'customer'  
            WHEN ticket_comments.dim_support_user_id = ticket_source.submitter_id THEN 'submitter'
            
            -- Filter out known automated/bot comments
            WHEN ticket_comments.via_channel = 'api' AND ticket_comments.dim_support_user_id IN (371478771020) THEN 'bot' -- Known automation user
            WHEN ticket_comments.ticket_comment_body_text LIKE '%This organization has an Assigned Support Engineer%' THEN 'bot'
            WHEN ticket_comments.ticket_comment_body_text LIKE '%Organization Notes%' THEN 'bot'
            WHEN ticket_comments.ticket_comment_body_text LIKE '%## This organization has%' THEN 'bot'
            
            -- Fallback classification for other users
            WHEN ticket_comments.via_channel = 'web' AND ticket_comments.dim_support_user_id != ticket_source.requester_id THEN 'likely_agent'
            WHEN ticket_comments.via_channel = 'api' AND ticket_comments.dim_support_user_id != ticket_source.requester_id THEN 'likely_agent'
            
            ELSE 'other'
        END AS commenter_type
        
    FROM ticket_comments
    INNER JOIN ticket_source ON ticket_comments.dim_support_ticket_id = ticket_source.ticket_id
    LEFT JOIN user_source ON ticket_comments.dim_support_user_id = user_source.user_id
    WHERE ticket_comments.is_public = TRUE  -- Only public comments count for FRT
),

-- Filter for legitimate agent responses for first reply time calculation
filtered_agent_comments AS (

    SELECT *
    FROM ticket_comments_with_roles 
    WHERE commenter_type IN ('gitlab_agent', 'assigned_agent')  -- Include all potential agents
        AND commenter_type != 'bot'  -- Exclude automated comments
        AND comment_created_at > ticket_created_at  -- Response must be after ticket creation

),

-- Calculate first reply time based on first legitimate agent response
first_reply_time AS (

    SELECT 
        ticket_id AS dim_support_ticket_id,
        MIN(comment_created_at) AS first_agent_response_at
    FROM filtered_agent_comments
    GROUP BY ticket_id

),

ticket_enriched AS (

    SELECT 
      -- Primary identifiers
      ticket_source.ticket_id           AS dim_support_ticket_id,
      ticket_source.external_id,
      
      -- Foreign keys
      ticket_source.assignee_id,
      ticket_source.brand_id,
      ticket_source.forum_topic_id,
      ticket_source.organization_id     AS dim_support_organization_id,
      ticket_source.requester_id,
      ticket_source.submitter_id,
      ticket_source.group_id,
      ticket_source.custom_status_id,
      ticket_source.problem_id,
      ticket_source.ticket_form_id      AS dim_support_ticket_form_id,
      
      -- Ticket attributes
      ticket_source.ticket_type,
      ticket_source.ticket_priority,
      ticket_source.ticket_status,
      ticket_source.recipient_email,
      ticket_source.has_incidents,
      ticket_source.is_public,
      ticket_source.allow_channelback,
      ticket_source.allow_attachments,
      ticket_source.is_from_messaging_channel,
      ticket_source.ticket_url,
      
      -- GitLab custom fields 
      ticket_source.area_of_focus,
      ticket_source.support_category,
      ticket_source.ticket_stage,
      ticket_source.company_name,
      ticket_source.support_ticket_category,
      ticket_source.gitlab_install_type,
      ticket_source.is_assignee_ooo,
      ticket_source.ticket_weight,
      ticket_source.gitlab_version,
      ticket_source.customer_priority,
      ticket_source.gitlab_plan,
      ticket_source.support_resolution_codes,
      ticket_source.ticket_arr,
      ticket_source.billing_region,
      ticket_source.sales_contact_email,
      ticket_source.sales_contact_name,
      ticket_source.subscription_email,
      ticket_source.ces_score,
      ticket_source.gitlab_issue_or_merge_request,
      ticket_source.gitlab_issue,
      ticket_source.gitlab_project_path,
      ticket_source.gitlab_namespace,
      
      -- Relationship arrays
      ticket_source.merged_ticket_ids,
      ticket_source.followup_ids,
      
      -- Timestamps
      ticket_source.created_at,
      ticket_source.updated_at,
      ticket_source.due_at,
      
      -- Add ticket tags (both formats for compatibility)
      ticket_tags.ticket_tags,
      ticket_tags.ticket_tags_array,
      
      -- Columns for downstream filtering with enhanced reduced effort logic
      IFF(ARRAYS_OVERLAP(ticket_tags.ticket_tags_array, ['closed_by_merge'::VARIANT]), TRUE, FALSE) AS is_duplicate,
      
      IFF(ARRAYS_OVERLAP(ticket_tags.ticket_tags_array, ['followup_ticket'::VARIANT]), TRUE, FALSE) AS is_follow_up,
      
      IFF(CONTAINS(ticket_tags.ticket_tags, 'autoresponder_'), TRUE, FALSE) AS is_2fa,
      
      IFF(
          CONTAINS(ticket_tags.ticket_tags, 'gitlab_issue_link')
          OR CONTAINS(ticket_tags.ticket_tags, 'gitlab_merge_request_link'),
          TRUE, 
          FALSE
      ) AS is_engineering_involved,

      -- Enhanced reduced effort classification using precise array matching
      IFF(ARRAYS_OVERLAP(ticket_tags.ticket_tags_array, [
              'autoclose-2fa-free'::VARIANT,
              'autoresponder_free_tickets'::VARIANT,
              'auto_reply_free_plan'::VARIANT,
              'agent_identified_free_user'::VARIANT,
              'autowork_account_blocked'::VARIANT,
              'autowork_no_confirmation_email'::VARIANT,
              'autowork_forgot_password'::VARIANT,
              'autoreply_saas_free'::VARIANT,
              'autoreply_prospect_free'::VARIANT,
              'close_unmonitored_inbox'::VARIANT,
              'autoclose_namesquatting_free'::VARIANT,
              'autoclose_security'::VARIANT,
              'autoresponder_gdpr'::VARIANT,
              'autoclose_sm_free'::VARIANT,
              'closed_unassociated_ticket'::VARIANT,
              'saas_account_access_issues_locked'::VARIANT,
              'submitted_via_gitlab_email'::VARIANT,
              'autoclose_nonapproved_users'::VARIANT
          ])
          OR (ARRAYS_OVERLAP(ticket_tags.ticket_tags_array, ['email_suppression_autochecked'::VARIANT]) 
              AND ARRAYS_OVERLAP(ticket_tags.ticket_tags_array, ['free_customer'::VARIANT]))
          OR (ARRAYS_OVERLAP(ticket_tags.ticket_tags_array, ['verification_requested'::VARIANT]) 
              AND ARRAYS_OVERLAP(ticket_tags.ticket_tags_array, ['stage-needsorg'::VARIANT])),
          TRUE, 
          FALSE) AS is_reduced_effort,

      -- Plan classification using precise array matching
      IFF(ARRAYS_OVERLAP(ticket_tags.ticket_tags_array, [
              'starter'::VARIANT, 'premium'::VARIANT, 'ultimate'::VARIANT, 
              'gold'::VARIANT, 'silver'::VARIANT, 'bronze'::VARIANT, 'basic'::VARIANT
          ]), 
          TRUE, 
          FALSE) AS has_plan,

      -- Resolution information with business logic applied
      ticket_resolution_time.solved_at,
      ticket_resolution_time.time_to_resolve_minutes,
      ROUND(ticket_resolution_time.time_to_resolve_minutes / 60.0, 2) AS time_to_resolve_hours,

      IFF(ticket_status in ('closed', 'solved'), TRUE, FALSE) AS is_closed,

      -- Improved First Reply Time calculation
      ROUND(DATEDIFF('second', ticket_source.created_at, first_reply_time.first_agent_response_at) / 60.0, 2) AS first_reply_time_minutes,
      ROUND(first_reply_time_minutes / 60.0, 2) AS first_reply_time_hours,

      -- Wait time calculations
      COALESCE(ticket_wait_times.requester_wait_time_minutes, 0) AS requester_wait_time_minutes,
      ROUND(requester_wait_time_minutes / 60.0, 2) AS requester_wait_time_hours,
      COALESCE(ticket_wait_times.customer_wait_time_minutes, 0) AS customer_wait_time_minutes,
      ROUND(customer_wait_time_minutes / 60.0, 2) AS customer_wait_time_hours,

      -- Customer Wait Time Ratio: CWT as percentage of TTR (time customer waits vs total resolution time)
      -- Formula: customer_wait_time_minutes / time_to_resolve_minutes  
      -- Example: 0.50 means 50% of total resolution time was spent with customer waiting for response
      IFF(
          COALESCE(ticket_resolution_time.time_to_resolve_minutes, 0) > 0,
          ROUND(COALESCE(ticket_wait_times.customer_wait_time_minutes, 0)::FLOAT / ticket_resolution_time.time_to_resolve_minutes, 2),
          NULL
      ) AS customer_wait_time_ratio

    FROM ticket_source
    LEFT JOIN ticket_tags
      ON ticket_source.ticket_id = ticket_tags.dim_support_ticket_id
    LEFT JOIN ticket_resolution_time
      ON ticket_source.ticket_id = ticket_resolution_time.dim_support_ticket_id
    LEFT JOIN first_reply_time
      ON ticket_source.ticket_id = first_reply_time.dim_support_ticket_id
    LEFT JOIN ticket_wait_times
      ON ticket_source.ticket_id = ticket_wait_times.dim_support_ticket_id

)

SELECT * 
FROM ticket_enriched