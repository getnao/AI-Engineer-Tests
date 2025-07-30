{{ simple_cte([
    ('prep_charge_base', 'prep_charge_base'),
    ('order_delta_mrr', 'zuora_query_api_order_delta_mrr_source'),
    ('charge_metrics', 'zuora_query_api_charge_metrics_source')
]) }},

charge_net_amounts_prep AS (

  SELECT
    prep_charge_base.dim_charge_id,
    prep_charge_base.rate_plan_charge_number,
    prep_charge_base.rate_plan_charge_segment,

    MAX(charge_metrics.mrr_net_amount)  AS charge_metrics_net_mrr,

    MAX(charge_metrics.tcv_net_amount)  AS charge_metrics_net_tcv,

    MAX(order_delta_mrr.mrr_net_amount) AS order_delta_mrr_net_amount

  FROM prep_charge_base
  LEFT JOIN charge_metrics
    ON prep_charge_base.dim_charge_id = charge_metrics.rate_plan_charge_id
  LEFT JOIN order_delta_mrr
    ON prep_charge_base.dim_charge_id = order_delta_mrr.rate_plan_charge_id
  GROUP BY prep_charge_base.dim_charge_id, prep_charge_base.rate_plan_charge_number, prep_charge_base.rate_plan_charge_segment

),

mrr_order_delta_and_charge_metrics AS (

  SELECT
    prep_charge_base.*,
    -- ADD: Net amount calculations using the hierarchy logic (orderdeltamrr -> chargemetrics -> mrr)
    ABS(COALESCE(
      charge_net_amounts_prep.order_delta_mrr_net_amount,
      charge_net_amounts_prep.charge_metrics_net_mrr,
      prep_charge_base.mrr  -- fallback to original mrr
    )) AS net_mrr,

    ABS(COALESCE(
      charge_net_amounts_prep.charge_metrics_net_tcv,
      prep_charge_base.tcv  -- fallback to original tcv
    )) AS net_tcv
  FROM prep_charge_base
  LEFT JOIN charge_net_amounts_prep
    ON prep_charge_base.dim_charge_id = charge_net_amounts_prep.dim_charge_id

),

mrr_replacement AS (

  SELECT
    *,

    -- Replace mrr with net_mrr only when mrr > net_mrr (indicating a discount exists) and after 2024-04-23 as before no order_delta_mrr or charge_metrics available
    CASE 
      WHEN mrr > net_mrr AND effective_start_date > '2024-04-23' THEN net_mrr
      ELSE mrr
    END AS final_mrr,
    
    -- Replace tcv with net_tcv only when mrr > net_mrr (indicating a discount exists) and after 2024-04-23 as before no order_delta_mrr or charge_metrics available
    CASE 
      WHEN mrr > net_mrr AND effective_start_date > '2024-04-23' THEN net_tcv
      ELSE tcv
    END AS final_tcv
  FROM mrr_order_delta_and_charge_metrics

),

non_discount_charges AS (

  SELECT *
  FROM mrr_replacement
  WHERE is_discount_charge = FALSE

)

SELECT *
FROM non_discount_charges
