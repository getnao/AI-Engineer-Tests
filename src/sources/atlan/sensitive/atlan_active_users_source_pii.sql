WITH source AS (

    SELECT *
    FROM {{ source('atlan', 'gitlab_active_users') }}

), renamed AS (

    SELECT
      active_date::TIMESTAMP    AS active_date,
      account_name::VARCHAR     AS account_name,
      user_id::VARCHAR          AS user_id,
      event_count::INT          AS event_count

    FROM source

)

SELECT *
FROM renamed