WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'container_fix_statuses') }}

),

renamed AS (

    SELECT

        --ids
        uuid                                                AS fix_status_id,
        container_uuid                                      AS container_id,
        operating_system_uuid                              AS operating_system_id,
        advisory_uuid                                       AS advisory_id,

        --fields
        fix_status                                          AS fix_status,

        --dates
        created_at,
        updated_at,
        loaded_at

    FROM source

)

SELECT *
FROM renamed