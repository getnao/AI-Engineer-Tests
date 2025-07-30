{{
  config(
    materialized = 'incremental',
    unique_key = 'fct_support_ticket_comment_sk',
    on_schema_change = 'sync_all_columns'
  )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('zendesk_fivetran_ticket_comment_source') }}
    
    {% if is_incremental() %}
      WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})
    {% endif %}

),

final AS (

  SELECT
      --ids
    {{ dbt_utils.generate_surrogate_key([
      'ticket_comment_id', 
      'created_at' 
    ]) }}                                 AS fct_support_ticket_comment_sk,
    ticket_comment_id                     AS dim_support_ticket_comment_id,
    ticket_id                             AS dim_support_ticket_id,
    user_id                               AS dim_support_user_id,

    --fields
    is_public,
    via_channel,
    ticket_comment_body_text,

    --dates
    created_at                           
  FROM source
)

SELECT *
FROM final