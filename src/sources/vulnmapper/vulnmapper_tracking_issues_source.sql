WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'tracking_issues') }}

),

renamed AS (

    SELECT

        --ids
        uuid                                                AS tracking_issue_id,
        projectuuid                                         AS project_id,
        provideruuid                                        AS provider_id,

        --fields
        id                                                  AS issue_id,
        url                                                 AS issue_url,
        title                                               AS issue_title,
        providertrackingstate                               AS provider_tracking_state,
        providerinternalid                                  AS provider_internal_id,
        owner                                               AS issue_owner,
        loaded_at                                           AS loaded_at

    FROM source

)

SELECT *
FROM renamed