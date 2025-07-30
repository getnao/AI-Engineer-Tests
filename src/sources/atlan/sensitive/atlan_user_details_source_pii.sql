WITH source AS (

    SELECT *
    FROM {{ source('atlan', 'gitlab_user_details') }}

), renamed AS (

    SELECT
      user_id::VARCHAR          AS user_id,
      signup_date::TIMESTAMP    AS signup_date,
      domain::VARCHAR           AS domain,
      role::VARCHAR             AS role,
      personas::VARCHAR         AS personas,
      groups::VARCHAR           AS groups,
      job_role::VARCHAR         AS job_role,
      data_platform::VARCHAR    AS data_platform,
      data_brands::VARCHAR      AS data_brands
      
    FROM source

)

SELECT *
FROM renamed