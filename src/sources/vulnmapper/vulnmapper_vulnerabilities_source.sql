WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'vulnerabilities') }}

),

renamed AS (

    SELECT

        --ids
        uuid                                                AS vulnerability_id,
        project_uuid                                        AS project_id,
        operating_system_uuid                               AS operating_system_id,
        provider_uuid                                       AS provider_id,

        --fields
        url                                                 AS vulnerability_url,
        description                                         AS vulnerability_description,
        location                                            AS vulnerability_location,
        scanner_type                                        AS scanner_type,
        finding_type                                        AS finding_type,
        provider_vuln_id                                    AS provider_tracking_id,
        severity                                            AS provider_tracking_severity,
        state                                               AS provider_tracking_state,

        --dates
        created_at                                          AS created_at,
        updated_at                                          AS updated_at,
        first_detected_at                                   AS first_detected_at,
        last_detected_at                                    AS last_detected_at,
        resolved_at                                         AS resolved_at,
        loaded_at                                           AS loaded_at

    FROM source

)

SELECT *
FROM renamed
