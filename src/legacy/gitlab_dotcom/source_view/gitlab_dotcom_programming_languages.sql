{{ config(
    snowflake_warehouse=generate_warehouse_name('XL')
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_programming_languages_source') }}

)

SELECT *
FROM source
