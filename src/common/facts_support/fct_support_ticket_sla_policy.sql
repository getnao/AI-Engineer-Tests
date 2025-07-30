{{
  config(
    materialized = 'incremental',
    unique_key = 'fct_support_ticket_sla_policy_sk',
    on_schema_change = 'sync_all_columns'
  )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('zendesk_fivetran_ticket_sla_policy_source') }}

    {% if is_incremental() %}
    WHERE policy_applied_at >= (SELECT MAX(sla_policy_applied_at) FROM {{ this }})
    {% endif %}

),

final AS (

  SELECT
    --ids
    {{ dbt_utils.generate_surrogate_key([
      'ticket_id', 
      'sla_policy_id', 
      'policy_applied_at'
    ]) }}                                               AS fct_support_ticket_sla_policy_sk,
    ticket_id                                           AS dim_support_ticket_id,
    sla_policy_id                                       AS dim_support_sla_policy_id,

    --dates
    policy_applied_at                                   AS sla_policy_applied_at
   
  FROM source
)

SELECT *
FROM final