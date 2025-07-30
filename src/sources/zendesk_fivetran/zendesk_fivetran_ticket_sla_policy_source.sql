WITH source AS (

    SELECT *
    FROM {{ source('zendesk_fivetran', 'ticket_sla_policy') }}

),

renamed AS (

    SELECT

        --ids
        ticket_id                                           AS ticket_id,
        sla_policy_id                                       AS sla_policy_id,

        --dates
        policy_applied_at                                   AS policy_applied_at,

        --metadata
        _fivetran_synced                                    AS synced_at

    FROM source

)

SELECT *
FROM renamed