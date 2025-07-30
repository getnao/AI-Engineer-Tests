WITH source AS (

    SELECT *
    FROM {{ source('zendesk_fivetran', 'satisfaction_rating') }}

),

renamed AS (

    SELECT

        --ids
        id                                                  AS satisfaction_rating_id,
        assignee_id                                         AS assignee_id,
        group_id                                            AS group_id,
        requester_id                                        AS requester_id,
        ticket_id                                           AS ticket_id,

        --fields
        url                                                 AS rating_url,
        score                                               AS satisfaction_score,
        comment                                             AS rating_comment,
        reason                                              AS rating_reason,

        --dates
        created_at,
        updated_at,

        --metadata
        _fivetran_synced                                    AS synced_at

    FROM source

)

SELECT *
FROM renamed