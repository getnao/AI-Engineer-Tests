WITH source AS (

    SELECT *
    FROM {{ source('zendesk_fivetran', 'ticket_comment') }}

),

renamed AS (

    SELECT

        --ids
        id                                                  AS ticket_comment_id,
        ticket_id                                           AS ticket_id,
        user_id                                             AS user_id,

        --fields
        body                                                AS ticket_comment_body_text, 
        public                                              AS is_public,
        via_channel                                         AS via_channel,
        --dates
        created                                             AS created_at,

        --metadata
        _fivetran_synced                                    AS synced_at

    FROM source

)

SELECT *
FROM renamed