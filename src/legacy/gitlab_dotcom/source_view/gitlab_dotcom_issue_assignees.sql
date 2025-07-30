{{ config(
    snowflake_warehouse=generate_warehouse_name('XL')
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issue_assignees_source') }}

)

SELECT *
FROM source
