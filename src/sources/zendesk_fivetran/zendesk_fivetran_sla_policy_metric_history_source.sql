WITH source AS (

    SELECT *
    FROM {{ source('zendesk_fivetran', 'sla_policy_metric_history') }}

),

renamed AS (

    SELECT

        --ids
        sla_policy_id                                       AS sla_policy_id,
        index                                               AS metric_index,

        --fields
        priority                                            AS metric_priority,
        metric                                              AS metric_type,
        target                                              AS target_hours,
        business_hours                                      AS uses_business_hours,

        --dates
        sla_policy_updated_at                               AS sla_policy_updated_at,

        --metadata
        _fivetran_synced                                    AS synced_at

    FROM source

)

SELECT *
FROM renamed