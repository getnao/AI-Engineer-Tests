WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'fy_mgp_targets') }}

), renamed as (

    SELECT
      role_level_1::VARCHAR                     AS role_level_1,
      role_level_2::VARCHAR                     AS role_level_2,
      geo::VARCHAR                              AS geo,
      region::VARCHAR                           AS region,
      fiscal_quarter::VARCHAR                   AS fiscal_quarter,
      fiscal_quarter_name_fy::VARCHAR           AS fiscal_quarter_name_fy,
      order_type::VARCHAR                       AS order_type,
      sales_qualified_source_name::VARCHAR      AS sales_qualified_source_name,
      mgp_contribution::VARCHAR                 AS mgp_contribution,
      include_in_target_attainment::BOOLEAN     AS include_in_target_attainment
    FROM source

)

SELECT *
FROM renamed
