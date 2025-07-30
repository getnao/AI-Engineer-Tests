{{ config(
    snowflake_warehouse=generate_warehouse_name('XL')
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_saml_providers_source') }}

)

SELECT *
FROM source
