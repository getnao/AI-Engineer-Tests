WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_static_targets_web_direct_fy26_source') }}

)

SELECT *
FROM source
