WITH source AS (
  SELECT *
  FROM {{ ref('gitlab_dotcom_incident_management_issuable_escalation_statuses_source') }}
)

SELECT * FROM source