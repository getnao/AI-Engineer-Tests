WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'vulnerabilities_servers_mapping') }}

),

renamed AS (

    SELECT

        --ids
        server_uuid                                         AS server_id,
        vulnerability_uuid                                  AS vulnerability_id,

        --dates
        loaded_at

    FROM source

)

SELECT *
FROM renamed