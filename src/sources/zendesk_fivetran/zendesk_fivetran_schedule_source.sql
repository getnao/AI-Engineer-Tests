WITH source AS (

    SELECT *
    FROM {{ source('zendesk_fivetran', 'schedule') }}

),

renamed AS (

    SELECT

        --ids
        id                                                  AS schedule_id,

        --fields
        name                                                AS schedule_name,
        time_zone                                           AS time_zone,
        
        --timestamps
        created_at                                          AS created_at,
        start_time                                          AS start_time,
        end_time                                            AS end_time,

        --metadata
        _fivetran_synced                                    AS synced_at

    FROM source

)

SELECT *
FROM renamed