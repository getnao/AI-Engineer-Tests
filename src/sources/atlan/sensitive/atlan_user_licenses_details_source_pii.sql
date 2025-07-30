WITH source AS (

    SELECT *
    FROM {{ source('atlan', 'gitlab_user_licenses_details') }}

), renamed AS (

    SELECT
      domain::VARCHAR        AS domain,
      date::DATE             AS recorded_date,
      event_name::VARCHAR    AS event_name,
      event_value::VARCHAR   AS event_value,
            

    FROM source

)

SELECT *
FROM renamed