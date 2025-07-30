WITH source AS (

    SELECT *
    FROM {{ source('zendesk_fivetran', 'ticket_field_history') }}

),

renamed AS (

    SELECT

        --ids
        ticket_id                                           AS ticket_id,
        user_id                                             AS user_id,

        --fields
        field_name                                          AS field_name,
        value                                               AS field_value,

        --dates
        updated                                             AS updated_at,

        --metadata
        _fivetran_synced                                    AS synced_at

    FROM source

)

SELECT *
FROM renamed