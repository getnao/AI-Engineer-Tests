WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'container_images') }}

),

renamed AS (

    SELECT

        --ids
        uuid                                                AS container_image_id,
        operating_system_uuid                              AS operating_system_id,
        provider_uuid                                       AS provider_id,

        --fields
        image_spec                                          AS image_specification,
        owner                                               AS image_owner,
        url                                                 AS image_url,

        --dates
        created_at,
        updated_at,
        loaded_at

    FROM source

)

SELECT *
FROM renamed
