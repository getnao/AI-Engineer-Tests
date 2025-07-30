 WITH source AS (

        SELECT * 
        FROM {{ source('sheetload','persona_mapping_keywords') }}

)

SELECT * 
FROM source