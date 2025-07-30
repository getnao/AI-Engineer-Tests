WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_fy_mgp_targets_source') }}

)

SELECT *
FROM source
