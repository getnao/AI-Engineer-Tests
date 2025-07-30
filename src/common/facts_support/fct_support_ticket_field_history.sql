{{
  config(
    materialized = 'incremental',
    unique_key = 'fct_support_ticket_field_change_id',
    on_schema_change = 'sync_all_columns'
  )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('prep_support_ticket_field_history') }}
    {% if is_incremental() %}
    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}

),

final AS (

  SELECT 
    fct_support_ticket_field_change_id,

    --Surrogate keys  
    dim_support_ticket_id,
    dim_support_user_id,
    
    -- Field change details
    field_name,
    field_value,
    previous_field_value,

    -- Date/time information
    updated_at,
    previous_field_updated_at
  FROM source
    
)

SELECT * 
FROM final