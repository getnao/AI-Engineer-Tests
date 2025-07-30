WITH source AS (

    SELECT *
    FROM {{ source('zendesk_fivetran', 'ticket_schedule') }}

),

renamed AS (

    SELECT

        --ids
        ticket_id                                           AS ticket_id,
        schedule_id                                         AS schedule_id,

        --timestamps
        created_at                                          AS created_at,

        --metadata
        _fivetran_synced                                    AS synced_at

    FROM source

)

SELECT *
FROM renamed