{{
  config(
    materialized = 'incremental',
    unique_key = 'fct_support_ticket_field_change_id',
    on_schema_change = 'sync_all_columns'
  )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('zendesk_fivetran_ticket_field_history_source') }}
    {% if is_incremental() %}
    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}

),

field_history_with_previous_value AS (
    SELECT 
        -- Primary identifiers
        ticket_id                 AS dim_support_ticket_id,
        user_id                   AS dim_support_user_id,
        updated_at,
        
        -- Field information
        field_name,
        field_value,
        
        -- Calculate previous field value & update time
        LAG(field_value) OVER (
            PARTITION BY ticket_id, field_name 
            ORDER BY updated_at, ticket_id
        ) AS previous_field_value,

        LAG(updated_at) OVER (
            PARTITION BY ticket_id, field_name 
            ORDER BY updated_at, ticket_id
        ) AS previous_field_updated_at
        
    FROM source
),

final AS (
  
    SELECT 
        {{ dbt_utils.generate_surrogate_key([
          'dim_support_ticket_id', 
          'field_name', 
          'updated_at'
        ]) }}                                 AS fct_support_ticket_field_change_id,
        field_history_with_previous_value.*
    FROM field_history_with_previous_value
)

SELECT * 
FROM final