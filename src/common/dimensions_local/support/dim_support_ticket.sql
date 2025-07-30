{{ simple_cte([
    ('source', 'prep_support_ticket')
]) }},

final AS (

  SELECT 
    -- Primary identifiers
    dim_support_ticket_id,

    -- Ticket attributes
    ticket_type,
    ticket_priority,
    ticket_status,
    recipient_email,
    has_incidents,
    allow_channelback,
    allow_attachments,
    ticket_url,
    ticket_tags,
    ticket_tags_array,

    -- GitLab custom fields 
    area_of_focus,
    support_category,
    ticket_stage,
    company_name,
    support_ticket_category,
    gitlab_install_type,
    ticket_weight,
    gitlab_version,
    customer_priority,
    gitlab_plan,
    support_resolution_codes,
    ticket_arr,
    billing_region,
    sales_contact_email,
    sales_contact_name,
    subscription_email,
    ces_score,
    gitlab_issue_or_merge_request,
    gitlab_issue,
    gitlab_project_path,
    gitlab_namespace


  FROM source

)

SELECT * 
FROM final