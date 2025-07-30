
WITH source AS (

    SELECT *
    FROM {{ source('zendesk_fivetran', 'schedule_holiday') }}

),

renamed AS (

    SELECT

        --ids
        id                                                  AS holiday_id,
        schedule_id                                         AS schedule_id,

        --fields
        name                                                AS holiday_name,
        
        --timestamps
        start_date                                          AS holiday_start_date_at,
        end_date                                            AS holiday_end_date_at,

        --metadata
        _fivetran_synced                                    AS synced_at

    FROM source

)

SELECT *
FROM renamed