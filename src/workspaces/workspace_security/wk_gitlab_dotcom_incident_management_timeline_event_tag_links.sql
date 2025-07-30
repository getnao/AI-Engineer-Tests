WITH source AS (
  SELECT *
  FROM {{ ref('gitlab_dotcom_incident_management_timeline_event_tag_links_source') }}
)

SELECT * FROM source