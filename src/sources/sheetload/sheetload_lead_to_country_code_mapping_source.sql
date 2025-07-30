WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'lead_to_country_code_mapping') }}

), renamed AS (

    SELECT
        country_name_variant::VARCHAR    AS country_name_variant,
        standard_country_name::VARCHAR   AS standard_country_name,
        country_iso_code::VARCHAR        AS country_iso_code
    FROM source

)

SELECT *
FROM renamed