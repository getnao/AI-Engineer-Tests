WITH source AS (

    SELECT *
    FROM {{ source('zuora', 'product_rate_plan_charge') }}

), renamed AS (

    SELECT 
      id::VARCHAR                    AS product_rate_plan_charge_id,
      productrateplanid::VARCHAR     AS product_rate_plan_id,
      name::VARCHAR                  AS product_rate_plan_charge_name,
      description::VARCHAR           AS product_rate_plan_charge_description,
      isseat__c::BOOLEAN             AS is_seat,
      chargetype::VARCHAR            AS charge_type,
      chargedelivery__c::VARCHAR     AS charge_delivery_type,
      chargedeployment__c::VARCHAR   AS charge_deployment_type,
      chargetier__c::VARCHAR         AS charge_tier
    FROM source
    
)

SELECT *
FROM renamed