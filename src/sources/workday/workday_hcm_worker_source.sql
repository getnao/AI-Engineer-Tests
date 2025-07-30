WITH source AS (

  SELECT *
  FROM {{ source('workday_hcm','worker') }}
  WHERE NOT _fivetran_deleted

),

final AS (

  SELECT
    id::VARCHAR                             AS id,
    user_id::VARCHAR                        AS employee_id,
    compensation_grade_id::VARCHAR          AS compensation_grade_id,
    compensation_grade_profile_id::VARCHAR  AS compensation_grade_profile_id
  FROM RAW.WORKDAY_HCM.WORKER
  -- Filter out contractor records which are identified by 'C-' prefix in their employee_id
  WHERE NOT (STARTSWITH(user_id, 'C-'))

)

SELECT *
FROM final