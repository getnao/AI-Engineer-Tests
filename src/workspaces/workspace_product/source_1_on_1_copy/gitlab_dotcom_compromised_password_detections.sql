WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_compromised_password_detections_source') }}

)

SELECT *
FROM source
