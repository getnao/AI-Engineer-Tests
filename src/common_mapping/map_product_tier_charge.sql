{{ simple_cte([
    ('zuora_product_rate_plan_charge_source','zuora_product_rate_plan_charge_source'),
    ('zuora_product_rate_plan_source','zuora_product_rate_plan_source'),
    ('zuora_product_source', 'zuora_product_source')
]) }}

, final AS (

    SELECT
      zuora_product_rate_plan_charge_source.product_rate_plan_charge_id,
      zuora_product_rate_plan_source.product_rate_plan_id,
      zuora_product_rate_plan_charge_source.product_rate_plan_charge_name,
      zuora_product_rate_plan_charge_source.product_rate_plan_charge_description,
      zuora_product_rate_plan_source.product_rate_plan_name,
      zuora_product_rate_plan_source.product_rate_plan_category,
      zuora_product_rate_plan_charge_source.is_seat,
      zuora_product_rate_plan_charge_source.charge_tier,
      zuora_product_rate_plan_charge_source.charge_delivery_type,
      zuora_product_rate_plan_charge_source.charge_deployment_type,
      zuora_product_source.category AS product_category,
      zuora_product_rate_plan_source.effective_start_date,
      zuora_product_rate_plan_source.effective_end_date
    FROM zuora_product_rate_plan_charge_source 
    INNER JOIN zuora_product_rate_plan_source 
      ON zuora_product_rate_plan_charge_source.product_rate_plan_id = zuora_product_rate_plan_source.product_rate_plan_id
    INNER JOIN zuora_product_source 
      ON zuora_product_rate_plan_source.product_id = zuora_product_source.product_id

)

SELECT *
FROM final
