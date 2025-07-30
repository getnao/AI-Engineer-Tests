WITH organization_source AS (
    SELECT *
    FROM {{ ref('zendesk_fivetran_organization_source') }}
),

final AS (
  SELECT
    organization_id             AS dim_support_organization_id,
    group_id                    AS dim_support_group_id,
    external_id,
    salesforce_account_id,

    -- Organization attributes
    organization_url,
    organization_name,
    organization_details,
    organization_notes,
    has_shared_tickets,
    has_shared_comments,
    solutions_architect,
    sales_segmentation,
    customer_success_manager,
    has_gitlab_com_premium,
    has_community_oss,
    account_owner,
    has_manual_support_upgrade,
    has_gitlab_duo_premium,
    has_professional_services,
    is_partner_customer,
    arr,
    contact_management_project_id,
    is_restricted_account,
    has_support_services_ase,
    am_project_id,
    has_us_government_24x7,
    number_of_seats,
    has_community_other,
    expiration_date,
    has_gitlab_duo_amazon_q,
    has_gitlab_com_ultimate,
    has_subscription_other,
    account_type,
    is_escalated,
    has_consumption_storage,
    health_score,
    has_gitlab_dedicated,
    is_support_hold,
    has_enterprise_agile_planning,
    has_self_managed_premium,
    gitlab_plan,
    assigned_se_id,
    region,
    has_self_managed_starter,
    has_community_edu,
    has_us_government_12x5,
    sfdc_short_id,
    has_gitlab_duo_enterprise,
    has_success_signature,
    has_consumption_ai,
    is_not_in_sfdc,
    is_greatly_expired,
    has_consumption_ci_cd_minutes,
    has_self_managed_ultimate,
    has_success_advanced,

    -- Date/time fields
    created_at,
    updated_at

  FROM organization_source
)

SELECT *
FROM final