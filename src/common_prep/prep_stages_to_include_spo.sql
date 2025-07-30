{{ config(
    tags=["product"],
    snowflake_warehouse=generate_warehouse_name('XL')
) }}

WITH source AS (

    SELECT DISTINCT stage_name
    FROM {{ ref('sheetload_usage_ping_metrics_sections') }}
    WHERE is_smau

)

SELECT *
FROM source
