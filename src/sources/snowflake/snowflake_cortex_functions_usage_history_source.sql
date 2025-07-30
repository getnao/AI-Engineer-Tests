WITH source AS (

  SELECT *
  FROM {{ source('snowflake_account_usage', 'cortex_functions_usage_history') }}

),

renamed AS (
SELECT
  start_time::TIMESTAMP AS cortex_function_start_at,
  end_time::TIMESTAMP AS cortex_function_end_at,
  function_name::VARCHAR AS cortex_function_name,
  model_name::VARCHAR AS cortex_model_name,
  warehouse_id::INT AS warehouse_id,
  token_credits::NUMBER(38,9) AS cortex_function_token_credits,  
  tokens::INT AS cortex_function_tokens 
FROM source

)

SELECT *
FROM renamed
