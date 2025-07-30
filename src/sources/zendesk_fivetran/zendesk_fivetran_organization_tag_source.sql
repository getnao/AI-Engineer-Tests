WITH source AS (

    SELECT *
    FROM {{ source('zendesk_fivetran', 'organization_tag') }}

),

renamed AS (

    SELECT

        --ids
        organization_id                                     AS organization_id,
        tag                                                 AS tag_name,

        --metadata
        _fivetran_synced                                    AS synced_at

    FROM source

)

SELECT *
FROM renamed