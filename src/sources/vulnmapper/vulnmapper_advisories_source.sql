
WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'advisories') }}

),

renamed AS (

    SELECT

        --ids
        uuid                                                AS advisory_id,
        provider_uuid                                       AS provider_id,

        --fields
        identifier                                          AS advisory_identifier,
        description                                         AS advisory_description,
        url                                                 AS advisory_url,
        score                                               AS advisory_score,
        impact                                              AS advisory_impact,
        vector                                              AS advisory_vector,

        --dates
        created_at,
        updated_at,
        loaded_at

    FROM source

)

SELECT *
FROM renamed
