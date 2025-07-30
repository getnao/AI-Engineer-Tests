{{ config(
    snowflake_warehouse=generate_warehouse_name('XL')
) }}

WITH source AS (

  SELECT * 
  FROM {{ ref('gitlab_dotcom_users_source') }}

), 

details AS (

  SELECT * 
  FROM {{ ref('gitlab_dotcom_user_details_source') }}

),

final AS (

  SELECT
    source.user_id,
    source.remember_created_at,
    source.sign_in_count,
    source.current_sign_in_at,
    source.last_sign_in_at,
    source.created_at,
    source.updated_at,
    source.is_admin,
    source.projects_limit,
    source.failed_attempts,
    source.locked_at,
    source.user_locked,
    source.has_create_group_permissions,
    source.has_create_team_permissions,
    source.state,
    source.color_scheme_id,
    source.password_expires_at,
    source.created_by_id,
    source.last_credential_check_at,
    source.has_avatar,
    source.confirmed_at,
    source.confirmation_sent_at,
    source.has_hide_no_ssh_key_enabled,
    source.admin_email_unsubscribed_at,
    source.notification_email,
    source.notification_email_domain,
    source.has_hide_no_password_enabled,
    source.is_password_automatically_set,
    source.location,
    source.email,
    source.email_domain,
    source.public_email,
    source.public_email_domain,
    source.commit_email,
    source.commit_email_domain,
    source.is_email_opted_in,
    source.email_opted_in_source_id,
    source.email_opted_in_at,
    source.dashboard,
    source.project_view,
    source.consumed_timestep,
    source.layout,
    source.has_hide_project_limit_enabled,
    source.otp_grace_period_started_at,
    source.is_external_user,
    source.organization,
    source.is_auditor,
    source.does_require_two_factor_authentication_from_group,
    source.two_factor_grace_period,
    source.last_activity_on,
    source.is_notified_of_own_activity,
    source.preferred_language,
    source.theme_id,
    source.accepted_term_id,
    source.is_private_profile,
    source.roadmap_layout,
    source.include_private_contributions,
    source.group_view,
    source.managing_group_id,
    details.role_id,
    details.role,
    source.user_name,
    source.first_name,
    source.last_name,
    source.users_name,
    source.user_type_id,
    source.user_type
  FROM source
  LEFT JOIN details
    ON source.user_id = details.user_id

)

SELECT *
FROM source
