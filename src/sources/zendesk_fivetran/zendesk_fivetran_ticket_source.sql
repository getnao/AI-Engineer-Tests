WITH source AS (

    SELECT *
    FROM {{ source('zendesk_fivetran', 'ticket') }}

),

renamed AS (

    SELECT

        --ids
        id                                                  AS ticket_id,
        assignee_id                                         AS assignee_id,
        brand_id                                            AS brand_id,
        forum_topic_id                                      AS forum_topic_id,
        organization_id                                     AS organization_id,
        requester_id                                        AS requester_id,
        submitter_id                                        AS submitter_id,
        group_id                                            AS group_id,
        custom_status_id                                    AS custom_status_id,
        external_id                                         AS external_id,
        problem_id                                          AS problem_id,
        ticket_form_id                                      AS ticket_form_id,
        custom_salesforce_case                              AS salesforce_case_id,

        --fields
        url                                                 AS ticket_url,
        type                                                AS ticket_type,
        priority                                            AS ticket_priority,
        status                                              AS ticket_status,
        subject                                             AS ticket_subject,
        description                                         AS ticket_description,
        recipient                                           AS recipient_email,
        has_incidents                                       AS has_incidents,
        is_public                                           AS is_public,
        allow_channelback                                   AS allow_channelback,
        allow_attachments                                   AS allow_attachments,
        from_messaging_channel                              AS is_from_messaging_channel,
        merged_ticket_ids                                   AS merged_ticket_ids,
        followup_ids                                        AS followup_ids,

        --system fields
        system_location                                     AS system_location,
        system_client                                       AS system_client,
        system_latitude                                     AS system_latitude,
        system_longitude                                    AS system_longitude,
        system_ip_address                                   AS system_ip_address,
        system_raw_email_identifier                         AS system_raw_email_identifier,
        system_eml_redacted                                 AS system_eml_redacted,
        system_json_email_identifier                        AS system_json_email_identifier,
        system_email_id                                     AS system_email_id,
        system_message_id                                   AS system_message_id,
        system_machine_generated                            AS system_machine_generated,
        system_ccs                                          AS system_ccs,

        --custom fields (selected key fields)
        custom_ticket_weight_for_sorting                    AS ticket_weight_for_sorting,
        custom_area_of_focus                                AS area_of_focus,
        custom_support_category                             AS support_category,
        custom_ticket_stage                                 AS ticket_stage,
        custom_company_name                                 AS company_name,
        custom_support_ticket_category                      AS support_ticket_category,
        custom_git_lab_install_type                         AS gitlab_install_type,
        custom_assignee_ooo                                 AS is_assignee_ooo,
        custom_ticket_weight                                AS ticket_weight,
        custom_git_lab_version                              AS gitlab_version,
        custom_customer_priority                            AS customer_priority,
        custom_git_lab_plan                                 AS gitlab_plan,
        custom_support_resolution_codes                     AS support_resolution_codes,
        custom_ticket_arr                                   AS ticket_arr,
        custom_billing_region                               AS billing_region,
        custom_sales_contact_email                          AS sales_contact_email,
        custom_sales_contact_name                           AS sales_contact_name,
        custom_subscription_email                           AS subscription_email,
        custom_ces_score                                    AS ces_score,
        custom_waiting_on_issue_or_merge_request            AS gitlab_issue_or_merge_request,
        custom_git_lab_issues                               AS gitlab_issue,
        custom_git_lab_com_project_path                     AS gitlab_project_path,
        custom_namespace                                    AS gitlab_namespace,

        --dates
        created_at,
        updated_at,
        due_at,

        --metadata
        _fivetran_synced                                    AS synced_at

    FROM source

)

SELECT *
FROM renamed