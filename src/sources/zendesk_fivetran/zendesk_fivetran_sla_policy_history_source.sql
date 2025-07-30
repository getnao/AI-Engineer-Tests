WITH source AS (

    SELECT *
    FROM {{ source('zendesk_fivetran', 'sla_policy_history') }}

),

renamed AS (

    SELECT

        --ids
        id                                                  AS sla_policy_id,

        --fields
        title                                               AS sla_policy_title,
        description                                         AS sla_policy_description,
        position                                            AS sla_policy_position,

        --dates
        created_at,
        updated_at,

        --metadata
        _fivetran_deleted                                   AS is_deleted,
        _fivetran_synced                                    AS synced_at

    FROM source

)

SELECT *
FROM renamed