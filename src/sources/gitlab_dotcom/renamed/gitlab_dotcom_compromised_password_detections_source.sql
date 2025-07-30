WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_compromised_password_detections_dedupe_source') }}

),

renamed AS (

  SELECT
    id::NUMBER AS compromised_password_detection_id,
    user_id::NUMBER AS user_id,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at,
    resolved_at::TIMESTAMP AS resolved_at
  FROM source 

)

SELECT * 
FROM renamed
