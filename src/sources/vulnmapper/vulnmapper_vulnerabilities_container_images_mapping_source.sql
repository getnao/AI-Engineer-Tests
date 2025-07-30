WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'vulnerabilities_container_images_mapping') }}

),

renamed AS (

    SELECT

        --ids
        vulnerability_uuid                                  AS vulnerability_id,
        container_uuid                                      AS container_id,

        --dates
        loaded_at

    FROM source

)

SELECT *
FROM renamed