WITH source AS (

  SELECT *
  FROM {{ source('zuora_query_api', 'orderdeltamrr') }}

), renamed AS (

  SELECT

    "Id"::TEXT                                                             AS order_delta_mrr_id,
    TRY_TO_DATE("StartDate"::TEXT)                                         AS start_date,
    TRY_TO_DATE("EndDate"::TEXT)                                           AS end_date,
    "GrossAmount"::NUMBER                                                  AS mrr_gross_amount,
    "NetAmount"::NUMBER                                                    AS mrr_net_amount,
    "OrderActionId"::TEXT                                                  AS order_action_id,
    "ChargeNumber"::TEXT                                                   AS charge_number,
    "RatePlanChargeId"::TEXT                                               AS rate_plan_charge_id,
    "ProductRatePlanChargeId"::TEXT                                        AS product_rate_plan_charge_id,
    "DELETED"::BOOLEAN                                                     AS is_deleted,  
    TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', "CreatedDate"))::TIMESTAMP        AS created_date,
    TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', "UpdatedDate"))::TIMESTAMP        AS updated_date,
    TO_TIMESTAMP_NTZ("_UPLOADED_AT"::INT)::TIMESTAMP                       AS uploaded_at

  FROM source  
    
)

SELECT *
FROM renamed