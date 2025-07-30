
WITH source AS (
  SELECT *
  FROM {{ ref('snowflake_cortex_functions_usage_history_source') }}

),

rates AS (
  SELECT 
    *, 
    LEAD(contract_rate_effective_date, 1, {{ var('tomorrow') }}) OVER (ORDER BY contract_rate_effective_date) AS next_contract_rate_effective_date
  FROM {{ ref('snowflake_contract_rates_source') }}
),


include_rates AS (

  SELECT
    source.cortex_function_start_at,
    source.cortex_function_end_at,
    source.cortex_function_name,
    source.cortex_model_name,
    source.cortex_function_token_credits,
    source.cortex_function_tokens,
    source.cortex_function_token_credits * rates.contract_rate AS dollars_used,
  FROM source
  LEFT JOIN rates
    ON source.cortex_function_start_at BETWEEN rates.contract_rate_effective_date AND rates.next_contract_rate_effective_date

)

SELECT *
FROM include_rates
