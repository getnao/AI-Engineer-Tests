{{ simple_cte([
    ('rpt_actuals','rpt_gtm_crm_actuals'),
    ('opportunity','mart_crm_opportunity'),
    ('targets','fct_sales_funnel_target_pivoted'),
    ('scaffold','rpt_gtm_scaffold'),
    ('user','dim_crm_user_hierarchy')
]) }},

actuals AS (

  SELECT
    rpt_actuals.dim_order_type_id,
    rpt_actuals.dim_sales_qualified_source_id,
    rpt_actuals.dim_crm_current_account_set_hierarchy_sk,
    rpt_actuals.actual_date_id,
    SUM(rpt_actuals.net_arr_pipeline_created)                                       AS net_arr_pipeline_created,
    SUM(rpt_actuals.booked_net_arr)                                                 AS booked_net_arr,
    SUM(rpt_actuals.first_order_booked_deals)                                       AS booked_new_logo_count,
    SUM(rpt_actuals.sao_count)                                                      AS sao_count,
    SUM(rpt_actuals.first_order_open_1plus_saos)                                    AS open_1plus_sao_count,
    SUM(rpt_actuals.first_order_open_1plus_saos_closing_current_fiscal_quarter)     AS open_1plus_saos_closing_current_fiscal_quarter_count,
    SUM(rpt_actuals.first_order_open_1plus_saos_closing_current_fiscal_year)        AS open_1plus_saos_closing_current_fiscal_year_count,
    SUM(rpt_actuals.first_order_open_3plus_saos_closing_current_fiscal_quarter)     AS open_3plus_saos_closing_current_fiscal_quarter_count,
    SUM(rpt_actuals.first_order_open_3plus_saos_closing_current_fiscal_year)        AS open_3plus_saos_closing_current_fiscal_year_count,
    SUM(rpt_actuals.first_order_open_1plus_pipeline)                                AS first_order_open_1plus_pipeline,
    SUM(rpt_actuals.first_order_open_3plus_pipeline)                                AS first_order_open_3plus_pipeline,
    SUM(rpt_actuals.first_order_open_1plus_pipeline_closing_current_fiscal_quarter) AS first_order_open_1plus_pipeline_closing_current_fiscal_quarter,
    SUM(rpt_actuals.first_order_open_3plus_pipeline_closing_current_fiscal_quarter) AS first_order_open_3plus_pipeline_closing_current_fiscal_quarter,
    SUM(rpt_actuals.first_order_open_1plus_pipeline_closing_current_fiscal_year)    AS first_order_open_1plus_pipeline_closing_current_fiscal_year,
    SUM(rpt_actuals.first_order_open_3plus_pipeline_closing_current_fiscal_year)    AS first_order_open_3plus_pipeline_closing_current_fiscal_year
  FROM rpt_actuals AS rpt_actuals
  LEFT JOIN opportunity as opportunity
  ON rpt_actuals.dim_crm_opportunity_id = opportunity.dim_crm_opportunity_id
  WHERE rpt_actuals.new_logo_count != 0
  AND opportunity.is_mid_market_plus = TRUE
  GROUP BY 1,2,3,4

)

, fo_targets AS (

  SELECT
    dim_order_type_id,
    dim_sales_qualified_source_id,
    targets.dim_crm_user_hierarchy_sk,
    target_date_id,
    SUM(new_logos_daily_allocated_target) AS new_logos_daily_allocated_target,
    SUM(new_logos_quarterly_allocated_target) AS new_logos_quarterly_allocated_target,
    SUM(new_logos_yearly_allocated_target) AS new_logos_yearly_allocated_target,
    SUM(new_logos_quarter_to_date_allocated_target) AS new_logos_quarter_to_date_allocated_target,
    SUM(new_logos_year_to_date_allocated_target) AS new_logos_year_to_date_allocated_target,
    SUM(saos_daily_allocated_target) AS saos_daily_allocated_target,
    SUM(saos_quarterly_allocated_target) AS saos_quarterly_allocated_target,
    SUM(saos_yearly_allocated_target) AS saos_yearly_allocated_target,
    SUM(saos_quarter_to_date_allocated_target) AS saos_quarter_to_date_allocated_target,
    SUM(saos_year_to_date_allocated_target) AS saos_year_to_date_allocated_target,
    SUM(net_arr_daily_allocated_target) AS net_arr_daily_allocated_target,
    SUM(net_arr_quarterly_allocated_target) AS net_arr_quarterly_allocated_target,
    SUM(net_arr_yearly_allocated_target) AS net_arr_yearly_allocated_target,
    SUM(net_arr_quarter_to_date_allocated_target) AS net_arr_quarter_to_date_allocated_target,
    SUM(net_arr_year_to_date_allocated_target) AS net_arr_year_to_date_allocated_target,
    SUM(net_arr_pipeline_created_daily_allocated_target) AS net_arr_pipeline_created_daily_allocated_target,
    SUM(net_arr_pipeline_created_quarterly_allocated_target) AS net_arr_pipeline_created_quarterly_allocated_target,
    SUM(net_arr_pipeline_created_yearly_allocated_target) AS net_arr_pipeline_created_yearly_allocated_target,
    SUM(net_arr_pipeline_created_quarter_to_date_allocated_target) AS net_arr_pipeline_created_quarter_to_date_allocated_target,
    SUM(net_arr_pipeline_created_year_to_date_allocated_target) AS net_arr_pipeline_created_year_to_date_allocated_target
  FROM targets as targets
  LEFT JOIN user
  ON targets.dim_crm_user_hierarchy_sk = user.dim_crm_user_hierarchy_sk
  WHERE dim_order_type_id = 'afc8fe87cec7435c0e7d6098d6ed1bb1' -- = 1. New - First Order
  AND user.crm_user_role_level_1 != 'SMB' OR user.crm_user_role_level_2 = 'SMB_BASE' -- filter to MM+
  GROUP BY 1,2,3,4

)

-- both CTEs are at the same grain, so if we bring them onto the scaffold together
-- and add some scaffold dimensional columns that do not change the grain
-- we effectively have a date/ dimension scaffold and a set of actuals and targets that can be combined without aggregation
SELECT
  scaffold.date_actual,
  scaffold.day_of_fiscal_quarter,
  scaffold.day_of_fiscal_year,
  scaffold.dim_crm_current_account_set_hierarchy_sk,
  scaffold.dim_order_type_id,
  scaffold.dim_sales_qualified_source_id,
  scaffold.fiscal_quarter_name_fy,
  scaffold.fiscal_year,
  scaffold.fiscal_month_name,
  '150' AS join_quarter_number, -- this is for forming a tableau relationship that is always true
  scaffold.first_day_of_fiscal_quarter,
  scaffold.first_day_of_fiscal_year,
  scaffold.crm_user_role_level_1,
  scaffold.crm_user_role_level_2,
  scaffold.crm_user_role_level_3,
  scaffold.sales_qualified_source_name,
  scaffold.order_type_name,
  actuals.net_arr_pipeline_created,
  actuals.booked_net_arr,
  actuals.booked_new_logo_count,
  actuals.sao_count,
  actuals.open_1plus_sao_count,
  actuals.open_1plus_saos_closing_current_fiscal_quarter_count,
  actuals.open_1plus_saos_closing_current_fiscal_year_count,
  actuals.open_3plus_saos_closing_current_fiscal_quarter_count,
  actuals.open_3plus_saos_closing_current_fiscal_year_count,
  actuals.first_order_open_1plus_pipeline,
  actuals.first_order_open_3plus_pipeline,
  actuals.first_order_open_1plus_pipeline_closing_current_fiscal_quarter,
  actuals.first_order_open_3plus_pipeline_closing_current_fiscal_quarter,
  actuals.first_order_open_1plus_pipeline_closing_current_fiscal_year,
  actuals.first_order_open_3plus_pipeline_closing_current_fiscal_year,
  targets.new_logos_daily_allocated_target,
  targets.new_logos_quarterly_allocated_target,
  targets.new_logos_yearly_allocated_target,
  targets.new_logos_quarter_to_date_allocated_target,
  targets.new_logos_year_to_date_allocated_target,
  targets.saos_daily_allocated_target,
  targets.saos_quarterly_allocated_target,
  targets.saos_yearly_allocated_target,
  targets.saos_quarter_to_date_allocated_target,
  targets.saos_year_to_date_allocated_target,
  targets.net_arr_daily_allocated_target,
  targets.net_arr_quarterly_allocated_target,
  targets.net_arr_yearly_allocated_target,
  targets.net_arr_quarter_to_date_allocated_target,
  targets.net_arr_year_to_date_allocated_target,
  targets.net_arr_pipeline_created_daily_allocated_target,
  targets.net_arr_pipeline_created_quarterly_allocated_target,
  targets.net_arr_pipeline_created_yearly_allocated_target,
  targets.net_arr_pipeline_created_quarter_to_date_allocated_target,
  targets.net_arr_pipeline_created_year_to_date_allocated_target
FROM scaffold
LEFT JOIN actuals
ON scaffold.date_id = actuals.actual_date_id
  AND scaffold.dim_order_type_id = actuals.dim_order_type_id
  AND scaffold.dim_sales_qualified_source_id = actuals.dim_sales_qualified_source_id
  AND scaffold.dim_crm_current_account_set_hierarchy_sk = actuals.dim_crm_current_account_set_hierarchy_sk
LEFT JOIN fo_targets AS targets
ON scaffold.date_id = targets.target_date_id
  AND scaffold.dim_order_type_id = targets.dim_order_type_id
  AND scaffold.dim_sales_qualified_source_id = targets.dim_sales_qualified_source_id
  AND scaffold.dim_crm_current_account_set_hierarchy_sk = targets.dim_crm_user_hierarchy_sk