WITH source AS (

  SELECT *
  FROM {{ source('snowflake_account_usage', 'cortex_functions_query_usage_history') }}

),

renamed AS (
SELECT
  query_id::VARCHAR AS query_id,
  warehouse_id::INT AS warehouse_id,
  model_name::VARCHAR AS cortex_model_name,
  function_name::VARCHAR AS cortex_function_name,
  tokens::INT AS cortex_function_tokens,
  token_credits::NUMBER(38,9) AS cortex_function_token_credits
FROM source

)

SELECT *
FROM renamed
