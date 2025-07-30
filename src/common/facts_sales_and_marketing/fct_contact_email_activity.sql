{{
  config(
    schema='common',
    tags=["mnpi_exception"]
  )
}}

WITH final AS (
    SELECT
        email_activity_pk,
        dim_person_sk,
        source_system,
        email_activity_type,
        activity_date,
        contact_email_address,
        email_activity_id,
        person_id,
        email_subject_line
    FROM {{ ref('prep_contact_email_activity') }}
)

SELECT *
FROM final