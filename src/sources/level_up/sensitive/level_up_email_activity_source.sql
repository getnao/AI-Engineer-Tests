WITH source AS (

  SELECT parse_json(jsontext) AS json_text,
         uploaded_at          AS uploaded_at
  FROM {{ source('level_up', 'email_activity') }}

),

final AS (

  SELECT 
    json_text:"email_activity.category"::VARCHAR        AS category,
    json_text:"email_activity.email"::VARCHAR           AS email,
    json_text:"email_activity.event"::VARCHAR           AS event,
    json_text:"email_activity.timestamp_time"::VARCHAR  AS timestamp_time,
    uploaded_at::TIMESTAMP                              AS uploaded_at 
  FROM source
) 

  SELECT *
  FROM final