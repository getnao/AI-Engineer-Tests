WITH source AS (

    SELECT *
    FROM {{ source('zendesk_fivetran', 'ticket_tag') }}

),

renamed AS (

    SELECT

        --ids
        ticket_id                                           AS ticket_id,

        --fields
        tag                                                 AS tag_name,

        --metadata
        _fivetran_synced                                    AS synced_at

    FROM source

)

SELECT *
FROM renamed