WITH source AS (
  SELECT *
  FROM {{ ref('gitlab_dotcom_incident_management_timeline_events_source') }}
)

SELECT * FROM source