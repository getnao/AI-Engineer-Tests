WITH source AS (

    SELECT *
    FROM {{ source('atlan', 'gitlab_event_details') }}

), renamed AS (

    SELECT
      usage_timestamp::TIMESTAMP    AS usage_timestamp,
      enrichment_event::VARCHAR     AS enrichment_event,
      link::VARCHAR                 AS enrichment_url,
      account_name::VARCHAR         AS account_name,
      role::VARCHAR                 AS user_role,
      user_id::VARCHAR              AS user_id,
      event_count::INT              AS event_count
      
    FROM source

)

SELECT *
FROM renamed
