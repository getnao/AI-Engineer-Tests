WITH source AS (

    SELECT 
      *,
      IFF(LOWER(last_name) LIKE '%integration%', 1, 0) AS is_integration_user,
      CASE WHEN zuora_user_id IN ('2c92a0107bde3653017bf00cd8a86d5a', '2c92a0fd55822b4d015593ac264767f2')
        THEN 1
        ELSE 0
      END AS is_self_service_integration_user
    FROM {{ref('zuora_query_api_users_source')}}

)

SELECT *
FROM source
