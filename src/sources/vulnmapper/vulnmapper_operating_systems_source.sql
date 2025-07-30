WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'operating_systems') }}

),

renamed AS (

    SELECT

        --ids
        uuid                                                AS operating_system_id,

        --fields
        operating_system_type                              AS os_type,
        operating_system_release                           AS os_release,
        operating_system_codename                          AS os_codename,

        --dates
        created_at,
        updated_at,
        loaded_at

    FROM source

)

SELECT *
FROM renamed