WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'tracking_issues_vulnerabilities_mapping') }}

),

renamed AS (

    SELECT

        --ids
        vulnerability_uuid                                  AS vulnerability_id,
        tracking_issue_uuid                                AS tracking_issue_id,

        --dates
        loaded_at

    FROM source

)

SELECT *
FROM renamed