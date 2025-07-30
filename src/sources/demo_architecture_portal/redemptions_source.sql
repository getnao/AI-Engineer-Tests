WITH source AS (

    SELECT *
    FROM {{ source('demo_architecture_portal', 'redemptions') }}

), renamed AS (
    
    SELECT
        id::NUMBER AS id,
        created_at::VARCHAR AS created_at,
        email::VARCHAR AS email,
        expiration_date::VARCHAR AS expiration_date,
        group_url::VARCHAR AS group_url,
        redemption_code::VARCHAR AS redemption_code,
        short_id::VARCHAR AS short_id,
        status::VARCHAR AS status,
        username::VARCHAR AS username,
        _sdc_batched_at::TIMESTAMP_TZ AS _sdc_batched_at,
        _sdc_received_at::TIMESTAMP_TZ AS _sdc_received_at,
        _sdc_sequence::NUMBER AS _sdc_sequence,
        _sdc_table_version::NUMBER AS _sdc_table_version
    FROM source
)

SELECT *
FROM renamed