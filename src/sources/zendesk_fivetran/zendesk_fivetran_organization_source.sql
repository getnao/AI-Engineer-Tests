WITH source AS (

    SELECT *
    FROM {{ source('zendesk_fivetran', 'organization') }}

),

renamed AS (

    SELECT

        --ids
        id                                                  AS organization_id,
        group_id                                            AS group_id,
        external_id                                         AS external_id,
        custom_salesforce_id                                AS salesforce_account_id,

        --fields
        url                                                 AS organization_url,
        name                                                AS organization_name,
        details                                             AS organization_details,
        notes                                               AS organization_notes,
        shared_tickets                                      AS has_shared_tickets,
        shared_comments                                     AS has_shared_comments,

        --custom fields
        custom_solutions_architect                          AS solutions_architect,
        custom_sales_segmentation                           AS sales_segmentation,
        custom_customer_success_manager                     AS customer_success_manager,
        custom_subscription_git_lab_com_premium             AS has_gitlab_com_premium,
        custom_subscription_community_oss                   AS has_community_oss,
        custom_account_owner                                AS account_owner,
        custom_manual_support_upgrade                       AS has_manual_support_upgrade,
        custom_subscription_consumption_git_lab_duo_premium AS has_gitlab_duo_premium,
        custom_subscription_professional_services           AS has_professional_services,
        custom_partner_customer                             AS is_partner_customer,
        custom_arr                                          AS arr,
        custom_contact_management_project_id                AS contact_management_project_id,
        custom_restricted_account                           AS is_restricted_account,
        custom_subscription_support_services_ase            AS has_support_services_ase,
        custom_am_project_id                                AS am_project_id,
        custom_subscription_us_government_24_x_7            AS has_us_government_24x7,
        custom_number_of_seats                              AS number_of_seats,
        custom_subscription_community_other                 AS has_community_other,
        custom_expiration_date                              AS expiration_date,
        custom_subscription_consumption_git_lab_duo_powered_by_amazon_q AS has_gitlab_duo_amazon_q,
        custom_subscription_git_lab_com_ultimate            AS has_gitlab_com_ultimate,
        custom_subscription_other                           AS has_subscription_other,
        custom_account_type                                 AS account_type,
        custom_escalated_state                              AS is_escalated,
        custom_subscription_consumption_storage             AS has_consumption_storage,
        custom_health_score                                 AS health_score,
        custom_subscription_git_lab_dedicated               AS has_gitlab_dedicated,
        custom_support_hold                                 AS is_support_hold,
        custom_subscription_consumption_enterprise_agile_planning AS has_enterprise_agile_planning,
        custom_subscription_self_managed_premium            AS has_self_managed_premium,
        custom_git_lab_plan                                 AS gitlab_plan,
        custom_assigned_se                                  AS assigned_se_id,
        custom_region                                       AS region,
        custom_subscription_self_managed_starter            AS has_self_managed_starter,
        custom_subscription_community_edu                   AS has_community_edu,
        custom_subscription_us_government_12_x_5            AS has_us_government_12x5,
        custom_sfdc_short_id                                AS sfdc_short_id,
        custom_subscription_consumption_git_lab_duo_enterprise AS has_gitlab_duo_enterprise,
        custom_subscription_support_services_success_signature AS has_success_signature,
        custom_subscription_consumption_ai                  AS has_consumption_ai,
        custom_sync_status_not_in_sfdc                      AS is_not_in_sfdc,
        custom_greatly_expired                              AS is_greatly_expired,
        custom_subscription_consumption_ci_cd_minutes       AS has_consumption_ci_cd_minutes,
        custom_subscription_self_managed_ultimate           AS has_self_managed_ultimate,
        custom_subscription_support_services_success_advanced AS has_success_advanced,

        --dates
        created_at,
        updated_at,

        --metadata
        _fivetran_synced                                    AS synced_at

    FROM source

)

SELECT *
FROM renamed