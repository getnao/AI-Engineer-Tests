WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'package_fix_statuses') }}

),

renamed AS (

    SELECT

        --ids
        uuid                                                AS package_fix_status_id,
        package_uuid                                        AS package_id,
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