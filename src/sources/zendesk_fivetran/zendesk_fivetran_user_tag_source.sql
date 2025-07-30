WITH source AS (

    SELECT *
    FROM {{ source('zendesk_fivetran', 'user_tag') }}

),

renamed AS (

    SELECT

        --ids
        user_id                                            AS user_id,
        tag                                                AS tag_name,

        --metadata
        _fivetran_synced                                   AS synced_at

    FROM source

)

SELECT *
FROM renamed