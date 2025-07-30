WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'fy_mql_targets') }}

)

SELECT
  fiscal_quarter::VARCHAR    AS fiscal_quarter,
  report_region::VARCHAR     AS report_region,
  report_geo::VARCHAR        AS report_geo,
  target::NUMBER             AS target
FROM source