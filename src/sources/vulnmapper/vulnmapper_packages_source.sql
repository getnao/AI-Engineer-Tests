WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'packages') }}

),

renamed AS (

    SELECT

        --ids
        uuid                                                AS package_id,
        operating_system_uuid                              AS operating_system_id,
        provider_uuid                                       AS provider_id,

        --fields
        name                                                AS package_name,
        version                                             AS package_version,
        arch                                                AS package_architecture,
        source_name                                         AS source_package_name,
        source_version                                      AS source_package_version,

        --dates
        created_at,
        updated_at,
        loaded_at

    FROM source

)

SELECT *
FROM renamed