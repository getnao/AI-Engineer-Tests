WITH source AS (
  SELECT *
  FROM {{ ref('gitlab_dotcom_incident_management_timeline_event_tags_source') }}
)

SELECT * FROM source