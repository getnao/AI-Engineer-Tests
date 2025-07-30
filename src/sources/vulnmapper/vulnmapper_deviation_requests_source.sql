WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'deviation_requests') }}

),

renamed AS (

    SELECT

        --ids
        uuid                                                AS deviation_request_id,
        provider_uuid                                       AS provider_id,

        --fields
        type                                                AS request_type,
        identifier                                          AS request_identifier,
        internal_id                                         AS internal_id,
        title                                               AS request_title,
        package_name                                        AS package_name,
        description                                         AS request_description,

        --dates
        created_at,
        updated_at,
        loaded_at

    FROM source

)

SELECT *
FROM renamed