WITH source AS (

    SELECT *
    FROM {{ source('vulnmapper', 'providers') }}

),

renamed AS (

    SELECT

        --ids
        uuid                                                AS provider_id,

        --fields
        name                                                AS provider_name,
        description                                         AS provider_description,

        --dates
        loaded_at

    FROM source

)

SELECT *
FROM renamed