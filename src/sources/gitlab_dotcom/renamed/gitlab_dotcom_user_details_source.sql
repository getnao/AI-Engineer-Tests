WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_user_details_dedupe_source') }}

),

renamed AS (

  SELECT
    user_id::NUMBER                                                   AS user_id,
    job_title::VARCHAR                                                AS job_title,
    organization::VARCHAR                                             AS user_organization,
    TRY_TO_NUMBER(discord)                                            AS user_discord,
    TRY_PARSE_JSON(onboarding_status):email_opt_in::VARCHAR           AS initial_email_opt_in_value,
    TRY_PARSE_JSON(onboarding_status):role::NUMBER                    AS role_id,
    TRY_PARSE_JSON(onboarding_status):setup_for_company::BOOLEAN      AS setup_for_company,
    TRY_PARSE_JSON(onboarding_status):registration_objective::NUMBER  AS registration_objective,
    {{ user_role_mapping(user_role='role_id') }}::VARCHAR             AS role,
    {{ it_job_title_hierarchy('role') }}
  FROM source

)

SELECT *
FROM renamed
