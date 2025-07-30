WITH source AS (

    SELECT *
    FROM {{ source('greenhouse', 'job_post_locations') }}

),

renamed AS (

    SELECT
        id::INTEGER                      AS job_post_location_id,
        name::VARCHAR                    AS job_post_location,
        type::VARCHAR                    AS input_type,
        job_post_id::INTEGER             AS job_post_id,
        organization_id::INTEGER         AS organization_id,
        created_at::TIMESTAMP_NTZ        AS job_post_location_created_at,
        updated_at::TIMESTAMP_NTZ        AS job_post_location_updated_at
    FROM source

)

SELECT *
FROM renamed
