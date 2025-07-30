{{ config(
    snowflake_warehouse=generate_warehouse_name('XL')
) }}

WITH source as (

    SELECT *
    FROM {{ source('gitlab_snowplow', 'bad_events') }}

)

SELECT *
FROM source
