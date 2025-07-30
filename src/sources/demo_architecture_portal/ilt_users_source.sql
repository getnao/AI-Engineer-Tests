WITH source AS (

    SELECT *
    FROM {{ source('demo_architecture_portal', 'ilt_users') }}

), renamed AS (
  
    SELECT
        id::NUMBER AS id,
        banned_at::TIMESTAMP_TZ AS banned_at,
        created_at::TIMESTAMP_TZ AS created_at,
        expires_at::TIMESTAMP_TZ AS expires_at,
        group_url::VARCHAR AS group_url,
        is_active::BOOLEAN AS is_active,
        is_banned::BOOLEAN AS is_banned,
        redemption_code::VARCHAR AS redemption_code,
        redemption_short_id::VARCHAR AS redemption_short_id,
        username::VARCHAR AS username,
        user_id::NUMBER AS user_id,
        _sdc_batched_at::TIMESTAMP_TZ AS _sdc_batched_at,
        _sdc_received_at::TIMESTAMP_TZ AS _sdc_received_at,
        _sdc_sequence::NUMBER AS _sdc_sequence,
        _sdc_table_version::NUMBER AS _sdc_table_version
    FROM source
)

SELECT *
FROM renamed