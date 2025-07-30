WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'servers') }}

),

renamed AS (

    SELECT

        --ids
        uuid                                                AS server_id,
        operating_system_uuid                              AS operating_system_id,
        provider_uuid                                       AS provider_id,

        --fields
        name                                                AS server_name,
        provider_server_id                                  AS provider_server_id,
        status                                              AS server_status,

        --dates
        created_at,
        updated_at,
        loaded_at

    FROM source

)

SELECT *
FROM renamed