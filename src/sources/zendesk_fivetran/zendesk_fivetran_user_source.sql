WITH source AS (

    SELECT *
    FROM {{ source('zendesk_fivetran', 'user') }}

),

renamed AS (

    SELECT

        --ids
        id                                                  AS user_id,
        custom_role_id                                      AS custom_role_id,
        default_group_id                                    AS default_group_id,
        organization_id                                     AS organization_id,

        --metadata
        _fivetran_synced                                    AS synced_at

    FROM source

)

SELECT *
FROM renamed