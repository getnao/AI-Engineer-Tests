WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'vulnerabilities_advisories_mapping') }}

),

renamed AS (

    SELECT

        --ids
        advisory_uuid                                         AS advisory_id,
        vulnerability_uuid                                  AS vulnerability_id,

        --dates
        loaded_at

    FROM source

)

SELECT *
FROM renamed
