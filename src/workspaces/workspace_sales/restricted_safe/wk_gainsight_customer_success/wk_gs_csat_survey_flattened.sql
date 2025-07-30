{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('csat_survey_flattened') }}

)

SELECT *
FROM source