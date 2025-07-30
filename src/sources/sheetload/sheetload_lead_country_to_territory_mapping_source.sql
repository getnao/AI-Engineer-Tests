WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'lead_country_to_territory_mapping') }}

), renamed AS (

    SELECT
        country_iso_code::VARCHAR AS country_iso_code,
        min_employees::INT        AS min_employees,
        max_employees::INT        AS max_employees,
        geo::VARCHAR              AS geo,
        region::VARCHAR           AS region,
        area::VARCHAR             AS area
    FROM source

)

SELECT *
FROM renamed