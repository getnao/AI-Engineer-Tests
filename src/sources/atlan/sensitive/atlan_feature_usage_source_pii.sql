WITH source AS (

    SELECT *
    FROM {{ source('atlan', 'gitlab_feature_usage') }}

), renamed AS (

    SELECT
      usage_timestamp::TIMESTAMP   AS usage_timestamp,
      feature_used::VARCHAR        AS feature_used,
      event_type::VARCHAR          AS event_type,
      link::VARCHAR                AS link,
      account_name::VARCHAR        AS account_name,
      asset_type::VARCHAR          AS asset_type,
      asset_connector::VARCHAR     AS asset_connector,
      asset_guid::VARCHAR          AS asset_guid,
      user_id::VARCHAR             AS user_id,
      role::VARCHAR                AS role,
      event_count::INT             AS event_count
      
    FROM source

)

SELECT *
FROM renamed