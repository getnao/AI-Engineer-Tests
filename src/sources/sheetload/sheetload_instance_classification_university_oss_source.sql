WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','instance_classification_university_oss') }}

), renamed AS (

    SELECT
      name::VARCHAR                             AS name,
      website::VARCHAR                          AS website,
      domain::VARCHAR                           AS domain,
      type::VARCHAR                             AS type
    FROM source

)

SELECT *
FROM renamed
