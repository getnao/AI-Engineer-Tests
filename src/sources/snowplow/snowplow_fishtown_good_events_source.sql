{{ config(
    snowflake_warehouse=generate_warehouse_name('XL')
) }}

WITH source as (

    SELECT *
    FROM {{ source('fishtown_snowplow', 'events') }}

)

SELECT *
FROM source
