{{
  config(
    materialized = 'incremental',
    unique_key = 'dim_support_ticket_form_id',
    on_schema_change = 'sync_all_columns'
  )
}}

WITH source AS (

  SELECT *
  FROM {{ ref('zendesk_fivetran_ticket_form_history_source') }}
  {% if is_incremental() %}
  WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})
  {% endif %}

),

final AS (

  SELECT
    -- ids
    ticket_form_id        AS dim_support_ticket_form_id,

    -- fields
    form_name,
    form_display_name,
    form_raw_name,

    -- dates
    created_at,
    updated_at

  FROM source
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dim_support_ticket_form_id
    ORDER BY updated_at DESC
    ) = 1 

)

SELECT *
FROM final