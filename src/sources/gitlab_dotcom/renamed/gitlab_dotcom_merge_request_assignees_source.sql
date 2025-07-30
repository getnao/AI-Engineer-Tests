WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_merge_request_assignees_dedupe_source') }}
  WHERE created_at IS NOT NULL

),

renamed AS (

  SELECT
    id::NUMBER                  AS merge_request_assignee_id,
    user_id::NUMBER             AS user_id,
    merge_request_id::NUMBER    AS merge_request_id,
    created_at::timestamp       AS created_at,
    project_id::NUMBER          AS project_id
  FROM source

)

SELECT *
FROM renamed
