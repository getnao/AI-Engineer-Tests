WITH source AS (

    SELECT *
    FROM {{ ref('runner_logs_source') }}

)
SELECT *
FROM source