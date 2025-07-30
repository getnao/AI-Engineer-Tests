{{ config(
    tags=["product"],
    snowflake_warehouse=generate_warehouse_name('XL')
) }}

{{ config({
    "materialized": "table",
    "unique_key": "dim_user_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('source', 'gitlab_dotcom_users_source'),
    ('email_classification', 'driveload_email_domain_classification_source'),
    ('identity','gitlab_dotcom_identities_source'),
    ('gitlab_dotcom_user_preferences','gitlab_dotcom_user_preferences_source'),
    ('gitlab_dotcom_user_details','gitlab_dotcom_user_details_source'),
    ('customers_db_leads_source','customers_db_leads_source'),
    ('highest_paid_subscription_plan','gitlab_dotcom_highest_paid_subscription_plan')
]) }},

email_classification_dedup AS (

  SELECT *
  FROM email_classification
  QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY domain DESC) = 1

),

closest_provider AS (

  SELECT
    source.user_id,
    identity.identity_provider
  FROM source
  LEFT JOIN identity
    ON source.user_id = identity.user_id
  WHERE
    identity.user_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY source.user_id
    ORDER BY TIMEDIFF(MILLISECONDS, source.created_at, COALESCE(identity.created_at, {{ var('infinity_future') }})) ASC
  ) = 1

),

user_preferences AS (

  SELECT
    user_id,
    COALESCE(early_access_program_participant::VARCHAR, 'Unknown') AS early_access_program_participant,
    COALESCE(timezone::VARCHAR, 'Unknown')                         AS timezone,
    COALESCE(text_editor_type::VARCHAR, 'Unknown')                 AS text_editor_type
  FROM gitlab_dotcom_user_preferences

),

user_details AS (

  SELECT
    user_id,
    CASE COALESCE(registration_objective, -1)
      WHEN 0 THEN 'basics'
      WHEN 1 THEN 'move_repository'
      WHEN 2 THEN 'code_storage'
      WHEN 3 THEN 'exploring'
      WHEN 4 THEN 'ci'
      WHEN 5 THEN 'other'
      WHEN 6 THEN 'joining_team'
      WHEN -1 THEN 'Unknown'
    END                                             AS jobs_to_be_done,
    initial_email_opt_in_value,
    job_title,
    registration_objective,
    role,
    role_id,
    COALESCE(setup_for_company::VARCHAR, 'Unknown') AS setup_for_company,
    it_job_title_hierarchy
  FROM gitlab_dotcom_user_details

),

customer_leads AS (

  SELECT
    user_id,
    COALESCE(MAX(is_for_business_use)::VARCHAR, 'Unknown') AS for_business_use,
    COALESCE(MAX(employees_bucket)::VARCHAR, 'Unknown')    AS employee_count,
    COALESCE(MAX(country)::VARCHAR, 'Unknown')             AS country,
    COALESCE(MAX(state)::VARCHAR, 'Unknown')               AS state
  FROM customers_db_leads_source
  GROUP BY
    user_id

),

renamed AS (

  SELECT
    --surrogate_key
    {{ dbt_utils.generate_surrogate_key(['source.user_id']) }} AS dim_user_sk,

    --natural_key
    source.user_id,

    --legacy natural_key to be deprecated during change management plan
    source.user_id                                                                              AS dim_user_id,

    --Other attributes
    source.email,
    user_details.it_job_title_hierarchy,
    source.remember_created_at,
    source.sign_in_count,
    source.current_sign_in_at,
    source.last_sign_in_at,
    source.created_at,
    dim_date.date_id                                                                            AS created_date_id,
    source.updated_at,
    source.state                                                                                AS user_state,
    TIMESTAMPDIFF(DAYS, source.created_at, CURRENT_TIMESTAMP(2))                                AS account_age,
    TIMESTAMPDIFF(DAYS, source.created_at, source.last_activity_on)                             AS days_from_account_creation_to_last_activity,
    CASE
      WHEN account_age <= 1 THEN '1 - 1 day or less'
      WHEN account_age <= 7 THEN '2 - 2 to 7 days'
      WHEN account_age <= 14 THEN '3 - 8 to 14 days'
      WHEN account_age <= 30 THEN '4 - 15 to 30 days'
      WHEN account_age <= 60 THEN '5 - 31 to 60 days'
      WHEN account_age > 60 THEN '6 - Over 60 days'
    END                                                                                         AS account_age_cohort,
    source.color_mode_id,
    CASE
      WHEN color_mode_id = 1 THEN 'Light'
      WHEN color_mode_id = 2 THEN 'Dark'
      WHEN color_mode_id = 3 THEN 'System'
    END                                                                                         AS color_mode,
    COALESCE(source.state IN ('blocked', 'banned'), FALSE)                                      AS is_blocked_user,
    source.notification_email_domain,
    notification_email_domain.classification                                                    AS notification_email_domain_classification,
    source.email_domain,
    email_domain.classification                                                                 AS email_domain_classification,
    source.public_email_domain,
    public_email_domain.classification                                                          AS public_email_domain_classification,
    source.commit_email_domain,
    commit_email_domain.classification                                                          AS commit_email_domain_classification,
    closest_provider.identity_provider,
    source.user_type_id,
    source.user_type,
    source.locked_at,
    source.preferred_language,
    source.user_locked,
    user_details.job_title,
    user_details.registration_objective,
    -- Both user_name and users_name fields are visible even in private profiles, so it is appropriate to include them in the dim_user table.
    source.user_name,
    source.users_name,
    source.public_email,
    user_details.initial_email_opt_in_value,
    source.created_by_id,
    source.group_view                                                                           AS group_view_id,
    CASE
      WHEN source.group_view = 1 THEN 'Details'
      WHEN source.group_view = 2 THEN 'Security Dashboard'
    END                                                                                         AS group_view,
    source.dashboard                                                                            AS dashboard_id,
    CASE
      WHEN source.dashboard = 0 THEN 'Projects'
      WHEN source.dashboard = 1 THEN 'Stars'
      WHEN source.dashboard = 2 THEN 'Project Activity'
      WHEN source.dashboard = 3 THEN 'Starred Project Activity'
      WHEN source.dashboard = 4 THEN 'Groups'
      WHEN source.dashboard = 5 THEN 'Todos'
      WHEN source.dashboard = 6 THEN 'Issues'
      WHEN source.dashboard = 7 THEN 'Merge Requests'
      WHEN source.dashboard = 8 THEN 'Operations'
      WHEN source.dashboard = 9 THEN 'Followed User Activity'
      WHEN source.dashboard = 10 THEN 'Your Activity'
      WHEN source.dashboard = 11 THEN 'Member Projects'
    END                                                                                         AS dashboard,
    source.layout                                                                               AS layout_id,
    CASE
      WHEN source.layout = 0 THEN 'Fixed'
      WHEN source.layout = 1 THEN 'Fluid'
    END                                                                                         AS layout,
    source.project_view                                                                         AS project_view_id,
    CASE
      WHEN source.project_view = 0 THEN 'Readme'
      WHEN source.project_view = 1 THEN 'Activity'
      WHEN source.project_view = 2 THEN 'Files'
      WHEN source.project_view = 3 THEN 'Wiki'
    END                                                                                         AS project_view,
    source.color_scheme_id,
    CASE
      WHEN source.color_scheme_id = 1 THEN 'Light'
      WHEN source.color_scheme_id = 2 THEN 'Dark'
      WHEN source.color_scheme_id = 3 THEN 'Solarized Light'
      WHEN source.color_scheme_id = 4 THEN 'Solarized Dark'
      WHEN source.color_scheme_id = 5 THEN 'Monokai'
      WHEN source.color_scheme_id = 6 THEN 'None'
    END                                                                                         AS color_scheme,
    source.theme_id,
    CASE
      WHEN source.theme_id = 1 THEN 'Indigo'
      WHEN source.theme_id = 2 THEN 'Gray'
      WHEN source.theme_id = 3 THEN 'Neutral'
      WHEN source.theme_id = 4 THEN 'Blue'
      WHEN source.theme_id = 5 THEN 'Green'
      WHEN source.theme_id = 6 THEN 'Light Indigo'
      WHEN source.theme_id = 7 THEN 'Light Blue'
      WHEN source.theme_id = 8 THEN 'Light Green'
      WHEN source.theme_id = 9 THEN 'Red'
      WHEN source.theme_id = 10 THEN 'Light Red'
      WHEN source.theme_id = 11 THEN 'Dark Mode'
    END                                                                                         AS theme,

    --flags
    source.is_admin,
    source.is_bot,
    source.is_auditor,
    source.has_create_group_permissions,
    source.has_create_team_permissions,
    source.has_hide_no_password_enabled,
    source.has_hide_no_ssh_key_enabled,
    source.has_hide_project_limit_enabled,
    source.include_private_contributions,
    source.is_external_user,
    source.is_notified_of_own_activity,
    source.is_private_profile,

    -- Expanded Attributes  (Not Found = Joined Row Not found for the Attribute)
    COALESCE(user_details.role, 'Unknown')                                                      AS role,
    COALESCE(TO_DATE(source.last_activity_on)::VARCHAR, 'Unknown')                              AS last_activity_date,
    COALESCE(TO_DATE(source.last_sign_in_at)::VARCHAR, 'Unknown')                               AS last_sign_in_date,
    COALESCE(user_details.setup_for_company, 'Not Found')                                       AS setup_for_company,
    COALESCE(user_preferences.early_access_program_participant, 'Not Found')                    AS early_access_program_participant,
    COALESCE(user_preferences.timezone, 'Not Found')                                            AS timezone,
    COALESCE(user_details.jobs_to_be_done, 'Not Found')                                         AS jobs_to_be_done,
    COALESCE(user_preferences.text_editor_type, 'Not Found')                                    AS text_editor_type,
    COALESCE(customer_leads.for_business_use, 'Not Found')                                      AS for_business_use,
    COALESCE(customer_leads.employee_count, 'Not Found')                                        AS employee_count,
    COALESCE(customer_leads.country, 'Not Found')                                               AS country,
    COALESCE(customer_leads.state, 'Not Found')                                                 AS state,

    -- Highest paid data background - https://gitlab.com/gitlab-data/analytics/-/merge_requests/9987#note_1929662333
    highest_paid_subscription_plan.highest_paid_subscription_plan_id
  FROM source
  LEFT JOIN dim_date
    ON TO_DATE(source.created_at) = dim_date.date_day
  LEFT JOIN email_classification_dedup AS notification_email_domain
    ON source.notification_email_domain = notification_email_domain.domain
  LEFT JOIN email_classification_dedup AS email_domain
    ON source.email_domain = email_domain.domain
  LEFT JOIN email_classification_dedup AS public_email_domain
    ON source.public_email_domain = public_email_domain.domain
  LEFT JOIN email_classification_dedup AS commit_email_domain
    ON source.commit_email_domain = commit_email_domain.domain
  LEFT JOIN closest_provider
    ON source.user_id = closest_provider.user_id
  LEFT JOIN user_preferences
    ON source.user_id = user_preferences.user_id
  LEFT JOIN user_details
    ON source.user_id = user_details.user_id
  LEFT JOIN customer_leads
    ON source.user_id = customer_leads.user_id
  LEFT JOIN highest_paid_subscription_plan
    ON source.user_id = highest_paid_subscription_plan.user_id

)

SELECT *
FROM renamed
