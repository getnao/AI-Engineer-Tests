 WITH source AS (

        SELECT * 
        FROM {{ source('sheetload','static_targets_web_direct_fy26') }}

        )
        SELECT * 
        FROM source