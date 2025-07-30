WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'vulnerabilities_packages_mapping') }}

),

renamed AS (

    SELECT

        --ids
        vulnerability_uuid                                  AS vulnerability_id,
        package_uuid                                        AS package_id,

        --dates
        loaded_at

    FROM source

)

SELECT *
FROM renamed