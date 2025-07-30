{{ config(
    materialized='table',
    tags=["six_hourly"]
) }}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('mart_crm_attribution_touchpoint', 'mart_crm_attribution_touchpoint'),
    ('mart_crm_opportunity_daily_snapshot', 'mart_crm_opportunity_daily_snapshot'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('mart_crm_account', 'mart_crm_account')
]) }}

, third_business_days AS (
  --Get all third business days with quarter information for both snapshot dates and current quarter logic
  SELECT
    date_day,
    fiscal_year,
    fiscal_quarter,
    fiscal_quarter_name_fy,
    first_day_of_fiscal_quarter,
    --LAG values for snapshot dates (looking at previous quarter from each 3rd business day)
    LAG(fiscal_year, 1) OVER (ORDER BY date_day)            AS snapshot_fiscal_year,
    LAG(fiscal_quarter, 1) OVER (ORDER BY date_day)         AS snapshot_fiscal_quarter,
    LAG(fiscal_quarter_name_fy, 1) OVER (ORDER BY date_day) AS snapshot_fiscal_quarter_name_fy,
    --Check if current date has passed this third business day
    CASE 
      WHEN CURRENT_DATE - 1 >= date_day 
        THEN TRUE 
      ELSE FALSE 
    END AS is_past_third_business_day,
    --Get the previous quarter name for live data logic
    LAG(fiscal_quarter_name_fy, 1) OVER (ORDER BY first_day_of_fiscal_quarter) AS previous_fiscal_quarter_name_fy
  FROM dim_date
  WHERE is_third_business_day_of_fiscal_quarter = 1
    AND date_day <= CURRENT_DATE + 90 

),

current_date_values AS (
  --Get current date values to be used across all rows
  SELECT
    dim_date.current_date_actual                              AS current_date_actual,
    dim_date.current_day_of_fiscal_quarter_normalised         AS current_day_of_fiscal_quarter,
    dim_date.current_day_of_fiscal_year                       AS current_day_of_fiscal_year,
    dim_date.fiscal_quarter_name_fy                           AS current_fiscal_quarter_name_fy,
    dim_date.fiscal_year                                      AS current_fiscal_year,
    third_business_days.is_past_third_business_day,
    third_business_days.previous_fiscal_quarter_name_fy
  FROM dim_date
  LEFT JOIN third_business_days
    ON dim_date.fiscal_quarter_name_fy = third_business_days.fiscal_quarter_name_fy
  WHERE dim_date.date_day = CURRENT_DATE - 1

),

quarters_to_use_live AS (
  --Determine which quarters should use live data
  SELECT 
    current_fiscal_quarter_name_fy AS fiscal_quarter_name_fy
  FROM current_date_values
  
  UNION
  
  SELECT 
    previous_fiscal_quarter_name_fy AS fiscal_quarter_name_fy
  FROM current_date_values
  WHERE is_past_third_business_day = FALSE
    AND previous_fiscal_quarter_name_fy IS NOT NULL

),

snapshot_dates AS (
  --Filter third business days for snapshot logic
  SELECT
    date_day,
    snapshot_fiscal_year AS fiscal_year,
    snapshot_fiscal_quarter AS fiscal_quarter,
    snapshot_fiscal_quarter_name_fy AS fiscal_quarter_name_fy
  FROM third_business_days
  WHERE date_day <= CURRENT_DATE - 1
    AND snapshot_fiscal_quarter_name_fy IS NOT NULL

),

mgp_opportunities AS (
  --Get MGP flag from attribution touchpoint (distinct needed to handle touchpoint grain)
  SELECT DISTINCT
    dim_crm_opportunity_id,
    is_mgp_opportunity
  FROM mart_crm_attribution_touchpoint
  WHERE is_mgp_opportunity = TRUE

),

--Historical quarters from snapshots
historical_opportunities AS (

  SELECT
    'Snapshot'                                                                                                                            AS data_source,
    snapshot_dates.fiscal_quarter_name_fy                                                                                                 AS snapshot_fiscal_quarter_name_fy,
    mart_crm_opportunity_daily_snapshot.pipeline_created_fiscal_quarter_date                                                              AS snapshot_fiscal_quarter_date,           
    mart_crm_opportunity_daily_snapshot.dim_crm_account_id,
    mart_crm_opportunity_daily_snapshot.dim_crm_opportunity_id,
    mart_crm_opportunity.sales_accepted_date,
    mart_crm_opportunity.sales_accepted_fiscal_quarter_name,
    mart_crm_opportunity.stage_0_pending_acceptance_date,
    mart_crm_opportunity.stage_0_pending_acceptance_month,
    mart_crm_opportunity.stage_0_pending_acceptance_fiscal_quarter_name,
    mart_crm_opportunity.stage_1_discovery_date,
    mart_crm_opportunity.stage_1_discovery_month,
    mart_crm_opportunity.stage_1_discovery_fiscal_quarter_name,
    mart_crm_opportunity.stage_2_scoping_date,
    mart_crm_opportunity.stage_2_scoping_month,
    mart_crm_opportunity.stage_2_scoping_fiscal_quarter_name,
    mart_crm_opportunity.stage_3_technical_evaluation_date,
    mart_crm_opportunity.stage_3_technical_evaluation_month,
    mart_crm_opportunity.stage_3_technical_evaluation_fiscal_quarter_name,
    --Use snapshot values for pipeline created dates
    mart_crm_opportunity_daily_snapshot.pipeline_created_date,
    mart_crm_opportunity_daily_snapshot.pipeline_created_month,
    mart_crm_opportunity_daily_snapshot.pipeline_created_fiscal_quarter_name,
    mart_crm_opportunity_daily_snapshot.pipeline_created_fiscal_quarter_date,
    mart_crm_opportunity_daily_snapshot.pipeline_created_fiscal_year,
    mart_crm_opportunity.days_in_1_discovery,
    mart_crm_opportunity.days_in_sao,
    mart_crm_opportunity.days_since_last_activity,
    --Use snapshot values for key fields
    mart_crm_opportunity_daily_snapshot.sales_qualified_source_name,
    mart_crm_opportunity.report_segment,
    mart_crm_opportunity_daily_snapshot.order_type,
    mart_crm_opportunity.order_type                                                                                                      AS order_type_live,
    mart_crm_opportunity_daily_snapshot.order_type_grouped,
    mart_crm_opportunity.report_geo,
    mart_crm_opportunity.report_region,
    mart_crm_opportunity.report_area,
    mart_crm_opportunity.report_geo_pubsec_segment,
    mart_crm_opportunity.parent_crm_account_geo_pubsec_segment,
    mart_crm_opportunity.report_role_level_1,
    mart_crm_opportunity.report_role_level_2,
    mart_crm_opportunity.report_role_level_3,
    mart_crm_opportunity.pipe_council_grouping,
    mart_crm_opportunity.parent_crm_account_territory,
    mart_crm_opportunity.parent_crm_account_sales_segment,
    mart_crm_opportunity.parent_crm_account_geo,
    mart_crm_opportunity.parent_crm_account_region,
    mart_crm_opportunity.parent_crm_account_area,
    mart_crm_opportunity.deal_path_name,
    mart_crm_opportunity.created_date,
    mart_crm_opportunity.close_date,
    mart_crm_opportunity_daily_snapshot.new_logo_count                                                                                   AS new_logo_count_snapshot,
    mart_crm_opportunity.new_logo_count,
    mart_crm_opportunity_daily_snapshot.calculated_deal_count                                                                            AS calculated_deal_count_snapshot,
    mart_crm_opportunity.calculated_deal_count  ,
    mart_crm_opportunity_daily_snapshot.opportunity_category                                                                             AS opportunity_category_snapshot,
    mart_crm_opportunity.opportunity_category,
    mart_crm_opportunity_daily_snapshot.stage_name                                                                                       AS stage_name_snapshot,
    mart_crm_opportunity.stage_name,
    mart_crm_opportunity.product_category,
    mart_crm_opportunity.product_details,
    mart_crm_opportunity.products_purchased,
    mart_crm_opportunity.crm_account_focus_account,
    mart_crm_opportunity_daily_snapshot.is_sao,
    mart_crm_opportunity.is_booked_net_arr,
    mart_crm_opportunity.is_net_arr_closed_deal,
    mart_crm_opportunity_daily_snapshot.is_net_arr_pipeline_created,
    mart_crm_opportunity_daily_snapshot.is_sales_dev_pipeline_created,
    mart_crm_opportunity.is_eligible_age_analysis,
    mart_crm_opportunity.is_eligible_open_pipeline,
    mart_crm_opportunity.fpa_master_bookings_flag,
    mart_crm_opportunity.is_stage_3_plus,
    mart_crm_opportunity.is_mid_market_plus,
    mart_crm_opportunity.is_closed,
    mart_crm_opportunity.is_won,
    mart_crm_opportunity.is_jihu_account,
    mart_crm_opportunity.is_sales_dev_qualified_opportunity,
    mgp_opportunities.is_mgp_opportunity,
    mart_crm_opportunity.crm_business_dev_rep_id,
    mart_crm_opportunity.crm_sales_dev_rep_id,
    mart_crm_opportunity_daily_snapshot.crm_business_dev_rep_id                                                                          AS snapshot_business_dev_rep_id,
    mart_crm_opportunity_daily_snapshot.crm_sales_dev_rep_id                                                                             AS snapshot_sales_dev_rep_id,
    mart_crm_opportunity_daily_snapshot.net_arr,
    mart_crm_opportunity.net_arr                                                                                                         AS net_arr_live,
    mart_crm_opportunity.net_arr_stage_1,
    mart_crm_opportunity.xdr_net_arr_stage_1,
    mart_crm_opportunity.xdr_net_arr_stage_3
  FROM mart_crm_opportunity_daily_snapshot
  INNER JOIN snapshot_dates
    ON mart_crm_opportunity_daily_snapshot.snapshot_date = snapshot_dates.date_day
  CROSS JOIN current_date_values
  LEFT JOIN mart_crm_opportunity
    ON mart_crm_opportunity_daily_snapshot.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN mgp_opportunities
    ON mart_crm_opportunity_daily_snapshot.dim_crm_opportunity_id = mgp_opportunities.dim_crm_opportunity_id
  LEFT JOIN quarters_to_use_live
    ON mart_crm_opportunity_daily_snapshot.pipeline_created_fiscal_quarter_name = quarters_to_use_live.fiscal_quarter_name_fy
  WHERE snapshot_dates.fiscal_quarter_name_fy = mart_crm_opportunity_daily_snapshot.pipeline_created_fiscal_quarter_name
    --Exclude quarters that should use live data
    AND quarters_to_use_live.fiscal_quarter_name_fy IS NULL 

),

--Current quarter from live data
current_quarter_opportunities AS (

  SELECT
    'Live'                                                                                                                                AS data_source,
    mart_crm_opportunity.pipeline_created_fiscal_quarter_name                                                                             AS snapshot_fiscal_quarter_name_fy,
    mart_crm_opportunity.pipeline_created_fiscal_quarter_date                                                                             AS snapshot_fiscal_quarter_date,           
    mart_crm_opportunity.dim_crm_account_id,
    mart_crm_opportunity.dim_crm_opportunity_id,
    mart_crm_opportunity.sales_accepted_date,
    mart_crm_opportunity.sales_accepted_fiscal_quarter_name,
    mart_crm_opportunity.stage_0_pending_acceptance_date,
    mart_crm_opportunity.stage_0_pending_acceptance_month,
    mart_crm_opportunity.stage_0_pending_acceptance_fiscal_quarter_name,
    mart_crm_opportunity.stage_1_discovery_date,
    mart_crm_opportunity.stage_1_discovery_month,
    mart_crm_opportunity.stage_1_discovery_fiscal_quarter_name,
    mart_crm_opportunity.stage_2_scoping_date,
    mart_crm_opportunity.stage_2_scoping_month,
    mart_crm_opportunity.stage_2_scoping_fiscal_quarter_name,
    mart_crm_opportunity.stage_3_technical_evaluation_date,
    mart_crm_opportunity.stage_3_technical_evaluation_month,
    mart_crm_opportunity.stage_3_technical_evaluation_fiscal_quarter_name,
    --Use live values for current quarter
    mart_crm_opportunity.pipeline_created_date,
    mart_crm_opportunity.pipeline_created_month,
    mart_crm_opportunity.pipeline_created_fiscal_quarter_name,
    mart_crm_opportunity.pipeline_created_fiscal_quarter_date,
    mart_crm_opportunity.pipeline_created_fiscal_year,
    mart_crm_opportunity.days_in_1_discovery,
    mart_crm_opportunity.days_in_sao,
    mart_crm_opportunity.days_since_last_activity,
    mart_crm_opportunity.sales_qualified_source_name,
    mart_crm_opportunity.report_segment,
    mart_crm_opportunity.order_type,
    mart_crm_opportunity.order_type                                                                                                      AS order_type_live,
    mart_crm_opportunity.order_type_grouped,
    mart_crm_opportunity.report_geo,
    mart_crm_opportunity.report_region,
    mart_crm_opportunity.report_area,
    mart_crm_opportunity.report_geo_pubsec_segment,
    mart_crm_opportunity.parent_crm_account_geo_pubsec_segment,
    mart_crm_opportunity.report_role_level_1,
    mart_crm_opportunity.report_role_level_2,
    mart_crm_opportunity.report_role_level_3,
    mart_crm_opportunity.pipe_council_grouping,
    mart_crm_opportunity.parent_crm_account_territory,
    mart_crm_opportunity.parent_crm_account_sales_segment,
    mart_crm_opportunity.parent_crm_account_geo,
    mart_crm_opportunity.parent_crm_account_region,
    mart_crm_opportunity.parent_crm_account_area,
    mart_crm_opportunity.deal_path_name,
    mart_crm_opportunity.created_date,
    mart_crm_opportunity.close_date,
    mart_crm_opportunity.new_logo_count                                                                                                  AS new_logo_count_snapshot,
    mart_crm_opportunity.new_logo_count,
    mart_crm_opportunity.calculated_deal_count                                                                                           AS calculated_deal_count_snapshot,
    mart_crm_opportunity.calculated_deal_count,
    mart_crm_opportunity.opportunity_category                                                                                            AS opportunity_category_snapshot,
    mart_crm_opportunity.opportunity_category,
    mart_crm_opportunity.stage_name                                                                                                      AS stage_name_snapshot,
    mart_crm_opportunity.stage_name,
    mart_crm_opportunity.product_category,
    mart_crm_opportunity.product_details,
    mart_crm_opportunity.products_purchased,
    mart_crm_opportunity.crm_account_focus_account,
    mart_crm_opportunity.is_sao,
    mart_crm_opportunity.is_booked_net_arr,
    mart_crm_opportunity.is_net_arr_closed_deal,
    mart_crm_opportunity.is_net_arr_pipeline_created,
    mart_crm_opportunity.is_sales_dev_pipeline_created,
    mart_crm_opportunity.is_eligible_age_analysis,
    mart_crm_opportunity.is_eligible_open_pipeline,
    mart_crm_opportunity.fpa_master_bookings_flag,
    mart_crm_opportunity.is_stage_3_plus,
    mart_crm_opportunity.is_mid_market_plus,
    mart_crm_opportunity.is_closed,
    mart_crm_opportunity.is_won,
    mart_crm_opportunity.is_jihu_account,
    mart_crm_opportunity.is_sales_dev_qualified_opportunity,
    mgp_opportunities.is_mgp_opportunity,
    mart_crm_opportunity.crm_business_dev_rep_id,
    mart_crm_opportunity.crm_sales_dev_rep_id,
    mart_crm_opportunity.crm_business_dev_rep_id                                                                                         AS snapshot_business_dev_rep_id,
    mart_crm_opportunity.crm_sales_dev_rep_id                                                                                            AS snapshot_sales_dev_rep_id,
    mart_crm_opportunity.net_arr,
    mart_crm_opportunity.net_arr                                                                                                         AS net_arr_live,
    mart_crm_opportunity.net_arr_stage_1,
    mart_crm_opportunity.xdr_net_arr_stage_1,
    mart_crm_opportunity.xdr_net_arr_stage_3
  FROM mart_crm_opportunity
  CROSS JOIN current_date_values
  LEFT JOIN mgp_opportunities
    ON mart_crm_opportunity.dim_crm_opportunity_id = mgp_opportunities.dim_crm_opportunity_id
  INNER JOIN quarters_to_use_live 
    ON mart_crm_opportunity.pipeline_created_fiscal_quarter_name = quarters_to_use_live.fiscal_quarter_name_fy

),

non_pipeline_opportunities AS (
    SELECT
    'Non - Pipeline'                                                                                                                      AS data_source,
    mart_crm_opportunity.pipeline_created_fiscal_quarter_name                                                                             AS snapshot_fiscal_quarter_name_fy,
    mart_crm_opportunity.pipeline_created_fiscal_quarter_date                                                                             AS snapshot_fiscal_quarter_date,           
    mart_crm_opportunity.dim_crm_account_id,
    mart_crm_opportunity.dim_crm_opportunity_id,
    mart_crm_opportunity.sales_accepted_date,
    mart_crm_opportunity.sales_accepted_fiscal_quarter_name,
    mart_crm_opportunity.stage_0_pending_acceptance_date,
    mart_crm_opportunity.stage_0_pending_acceptance_month,
    mart_crm_opportunity.stage_0_pending_acceptance_fiscal_quarter_name,
    mart_crm_opportunity.stage_1_discovery_date,
    mart_crm_opportunity.stage_1_discovery_month,
    mart_crm_opportunity.stage_1_discovery_fiscal_quarter_name,
    mart_crm_opportunity.stage_2_scoping_date,
    mart_crm_opportunity.stage_2_scoping_month,
    mart_crm_opportunity.stage_2_scoping_fiscal_quarter_name,
    mart_crm_opportunity.stage_3_technical_evaluation_date,
    mart_crm_opportunity.stage_3_technical_evaluation_month,
    mart_crm_opportunity.stage_3_technical_evaluation_fiscal_quarter_name,
    --Use live values for current quarter
    mart_crm_opportunity.pipeline_created_date,
    mart_crm_opportunity.pipeline_created_month,
    mart_crm_opportunity.pipeline_created_fiscal_quarter_name,
    mart_crm_opportunity.pipeline_created_fiscal_quarter_date,
    mart_crm_opportunity.pipeline_created_fiscal_year,
    mart_crm_opportunity.days_in_1_discovery,
    mart_crm_opportunity.days_in_sao,
    mart_crm_opportunity.days_since_last_activity,
    mart_crm_opportunity.sales_qualified_source_name,
    mart_crm_opportunity.report_segment,
    mart_crm_opportunity.order_type,
    mart_crm_opportunity.order_type                                                                                                      AS order_type_live,
    mart_crm_opportunity.order_type_grouped,
    mart_crm_opportunity.report_geo,
    mart_crm_opportunity.report_region,
    mart_crm_opportunity.report_area,
    mart_crm_opportunity.report_geo_pubsec_segment,
    mart_crm_opportunity.parent_crm_account_geo_pubsec_segment,
    mart_crm_opportunity.report_role_level_1,
    mart_crm_opportunity.report_role_level_2,
    mart_crm_opportunity.report_role_level_3,
    mart_crm_opportunity.pipe_council_grouping,
    mart_crm_opportunity.parent_crm_account_territory,
    mart_crm_opportunity.parent_crm_account_sales_segment,
    mart_crm_opportunity.parent_crm_account_geo,
    mart_crm_opportunity.parent_crm_account_region,
    mart_crm_opportunity.parent_crm_account_area,
    mart_crm_opportunity.deal_path_name,
    mart_crm_opportunity.created_date,
    mart_crm_opportunity.close_date,
    mart_crm_opportunity.new_logo_count                                                                                                  AS new_logo_count_snapshot,
    mart_crm_opportunity.new_logo_count,
    mart_crm_opportunity.calculated_deal_count                                                                                           AS calculated_deal_count_snapshot,
    mart_crm_opportunity.calculated_deal_count,
    mart_crm_opportunity.opportunity_category                                                                                            AS opportunity_category_snapshot,
    mart_crm_opportunity.opportunity_category,
    mart_crm_opportunity.stage_name                                                                                                      AS stage_name_snapshot,
    mart_crm_opportunity.stage_name,
    mart_crm_opportunity.product_category,
    mart_crm_opportunity.product_details,
    mart_crm_opportunity.products_purchased,
    mart_crm_opportunity.crm_account_focus_account,
    mart_crm_opportunity.is_sao,
    mart_crm_opportunity.is_booked_net_arr,
    mart_crm_opportunity.is_net_arr_closed_deal,
    0                                                                                                                                    AS is_net_arr_pipeline_created,
    0                                                                                                                                    AS is_sales_dev_pipeline_created,
    mart_crm_opportunity.is_eligible_age_analysis,
    mart_crm_opportunity.is_eligible_open_pipeline,
    mart_crm_opportunity.fpa_master_bookings_flag,
    mart_crm_opportunity.is_stage_3_plus,
    mart_crm_opportunity.is_mid_market_plus,
    mart_crm_opportunity.is_closed,
    mart_crm_opportunity.is_won,
    mart_crm_opportunity.is_jihu_account,
    mart_crm_opportunity.is_sales_dev_qualified_opportunity,
    mgp_opportunities.is_mgp_opportunity,
    mart_crm_opportunity.crm_business_dev_rep_id,
    mart_crm_opportunity.crm_sales_dev_rep_id,
    mart_crm_opportunity.crm_business_dev_rep_id                                                                                         AS snapshot_business_dev_rep_id,
    mart_crm_opportunity.crm_sales_dev_rep_id                                                                                            AS snapshot_sales_dev_rep_id,
    0                                                                                                                                    AS net_arr,
    mart_crm_opportunity.net_arr                                                                                                         AS net_arr_live,
    mart_crm_opportunity.net_arr_stage_1,
    mart_crm_opportunity.xdr_net_arr_stage_1,
    mart_crm_opportunity.xdr_net_arr_stage_3
  FROM mart_crm_opportunity
  LEFT JOIN current_quarter_opportunities
    ON mart_crm_opportunity.dim_crm_opportunity_id = current_quarter_opportunities.dim_crm_opportunity_id
  LEFT JOIN historical_opportunities
    ON mart_crm_opportunity.dim_crm_opportunity_id = historical_opportunities.dim_crm_opportunity_id
  CROSS JOIN current_date_values
  LEFT JOIN mgp_opportunities
    ON mart_crm_opportunity.dim_crm_opportunity_id = mgp_opportunities.dim_crm_opportunity_id
  WHERE current_quarter_opportunities.dim_crm_opportunity_id IS NULL 
  AND historical_opportunities.dim_crm_opportunity_id IS NULL
  
),

--Union historical and current data
combined_opportunities AS (

  SELECT * FROM historical_opportunities

  UNION ALL

  SELECT * FROM current_quarter_opportunities

  UNION ALL 

  SELECT * FROM non_pipeline_opportunities 

)

SELECT
  combined_opportunities.data_source,
  combined_opportunities.snapshot_fiscal_quarter_name_fy,
  combined_opportunities.snapshot_fiscal_quarter_date,
  combined_opportunities.dim_crm_account_id,
  mart_crm_account.bdr_prospecting_status,
  combined_opportunities.dim_crm_opportunity_id,
  ROW_NUMBER() OVER (
    PARTITION BY combined_opportunities.dim_crm_opportunity_id 
    ORDER BY combined_opportunities.close_date DESC
  )                                                                                                                                      AS opp_row_num,
  combined_opportunities.sales_accepted_date,
  combined_opportunities.sales_accepted_fiscal_quarter_name,
  stage_1_date.day_of_fiscal_quarter_normalised                                                                                          AS sao_day_of_fiscal_quarter,
  stage_1_date.day_of_fiscal_year_normalised                                                                                             AS sao_day_of_fiscal_year,
  stage_1_date.fiscal_quarters_ago                                                                                                       AS sao_fiscal_quarters_ago,
  pipeline_date.day_of_fiscal_quarter_normalised                                                                                         AS pipeline_day_of_fiscal_quarter,
  pipeline_date.day_of_fiscal_year_normalised                                                                                            AS pipeline_day_of_fiscal_year,
  pipeline_date.fiscal_quarters_ago                                                                                                      AS pipeline_fiscal_quarters_ago,
  combined_opportunities.stage_0_pending_acceptance_date,
  combined_opportunities.stage_0_pending_acceptance_month,
  combined_opportunities.stage_0_pending_acceptance_fiscal_quarter_name,
  combined_opportunities.stage_1_discovery_date,
  combined_opportunities.stage_1_discovery_month,
  combined_opportunities.stage_1_discovery_fiscal_quarter_name,
  combined_opportunities.stage_2_scoping_date,
  combined_opportunities.stage_2_scoping_month,
  combined_opportunities.stage_2_scoping_fiscal_quarter_name,
  combined_opportunities.stage_3_technical_evaluation_date,
  combined_opportunities.stage_3_technical_evaluation_month,
  combined_opportunities.stage_3_technical_evaluation_fiscal_quarter_name,
  combined_opportunities.pipeline_created_date,
  combined_opportunities.pipeline_created_month,
  combined_opportunities.pipeline_created_fiscal_quarter_name,
  combined_opportunities.pipeline_created_fiscal_quarter_date,
  combined_opportunities.pipeline_created_fiscal_year,
  combined_opportunities.days_in_1_discovery,
  combined_opportunities.days_in_sao,
  combined_opportunities.days_since_last_activity,
  combined_opportunities.sales_qualified_source_name,
  IFF(combined_opportunities.sales_qualified_source_name = 'SDR Generated','SDR Generated', 'Non-SDR Generated')                           AS sdr_sqs_or_not,                                                                                                                     
  combined_opportunities.report_segment,
  combined_opportunities.order_type,
  combined_opportunities.order_type_live,
  combined_opportunities.order_type_grouped,
  IFF(combined_opportunities.order_type = '2. New - Connected', '3. Growth', combined_opportunities.order_type)                           AS order_type_target_match,
  combined_opportunities.report_geo,
  combined_opportunities.report_region,
  combined_opportunities.report_area,
  combined_opportunities.report_geo_pubsec_segment,
  combined_opportunities.parent_crm_account_geo_pubsec_segment,
  combined_opportunities.report_role_level_1,
  combined_opportunities.report_role_level_2,
  combined_opportunities.report_role_level_3,
  combined_opportunities.pipe_council_grouping,
  combined_opportunities.parent_crm_account_territory,
  combined_opportunities.parent_crm_account_sales_segment,
  combined_opportunities.parent_crm_account_geo,
  combined_opportunities.parent_crm_account_region,
  combined_opportunities.parent_crm_account_area,
  combined_opportunities.deal_path_name,
  combined_opportunities.created_date,
  combined_opportunities.close_date,
  close_date.day_of_fiscal_quarter_normalised                                                                                            AS close_day_of_fiscal_quarter,
  close_date.day_of_fiscal_year_normalised                                                                                               AS close_day_of_fiscal_year,
  close_date.fiscal_quarter_name_fy                                                                                                      AS close_fiscal_quarter_name,
  close_date.first_day_of_fiscal_quarter                                                                                                 AS close_fiscal_quarter_date,
  close_date.fiscal_quarters_ago                                                                                                         AS close_fiscal_quarters_ago,
  close_date.fiscal_year                                                                                                                 AS close_fiscal_year,
  current_date_values.current_date_actual,
  current_date_values.current_day_of_fiscal_quarter,
  current_date_values.current_day_of_fiscal_year,
  combined_opportunities.new_logo_count_snapshot,
  IFF(opp_row_num = 1, combined_opportunities.new_logo_count, 0)                                                                         AS new_logo_count,
  combined_opportunities.calculated_deal_count_snapshot,
  IFF(opp_row_num = 1, combined_opportunities.calculated_deal_count, 0)                                                                  AS calculated_deal_count,
  CASE 
    WHEN opp_row_num = 1 
      AND combined_opportunities.order_type = '1. New - First Order' 
      THEN combined_opportunities.new_logo_count
    WHEN opp_row_num = 1 
      THEN combined_opportunities.calculated_deal_count
    ELSE 0
  END                                                                                                                                    AS combined_deal_count,
  IFF(combined_opportunities.order_type = '1. New - First Order', new_logo_count_snapshot, calculated_deal_count_snapshot)               AS combined_deal_count_snapshot,
  combined_opportunities.opportunity_category_snapshot,
  combined_opportunities.opportunity_category,
  combined_opportunities.stage_name_snapshot,
  combined_opportunities.stage_name,
  combined_opportunities.product_category,
  combined_opportunities.product_details,
  combined_opportunities.products_purchased,
  combined_opportunities.crm_account_focus_account,
  combined_opportunities.is_sao,
  combined_opportunities.is_booked_net_arr,
  combined_opportunities.is_net_arr_closed_deal,
  combined_opportunities.is_net_arr_pipeline_created,
  combined_opportunities.is_eligible_age_analysis,
  combined_opportunities.is_eligible_open_pipeline,
  combined_opportunities.fpa_master_bookings_flag,
  combined_opportunities.is_stage_3_plus,
  combined_opportunities.is_mid_market_plus,
  combined_opportunities.is_closed,
  combined_opportunities.is_won,
  combined_opportunities.is_jihu_account,
  combined_opportunities.is_mgp_opportunity,
  combined_opportunities.crm_business_dev_rep_id,
  combined_opportunities.crm_sales_dev_rep_id,
  mart_crm_account.bdr_next_steps,
  mart_crm_account.bdr_account_research,
  mart_crm_account.bdr_account_strategy,
  mart_crm_account.account_bdr_assigned_user_role,
  mart_crm_account.bdr_recycle_date,
  mart_crm_account.actively_working_start_date,
  CASE
    WHEN combined_opportunities.new_logo_count <> 0
      AND combined_opportunities.is_booked_net_arr = TRUE
      AND combined_opportunities.sales_qualified_source_name = 'SDR Generated'
      AND combined_opportunities.opportunity_category <> 'Credit'
      AND opp_row_num = 1
      THEN combined_opportunities.new_logo_count
    ELSE 0
  END                                                                                                                                     AS is_sdr_first_order_booked_deal,
  CASE
    WHEN combined_opportunities.crm_business_dev_rep_id IS NOT NULL
      THEN 'BDR'
    WHEN combined_opportunities.crm_sales_dev_rep_id IS NOT NULL
      THEN 'SDR'
  END                                                                                                                                     AS sales_dev_bdr_or_sdr,
  COALESCE(
    combined_opportunities.snapshot_business_dev_rep_id,
    combined_opportunities.snapshot_sales_dev_rep_id,
    combined_opportunities.crm_business_dev_rep_id,
    combined_opportunities.crm_sales_dev_rep_id
  )                                                                                                                                       AS sdr_bdr_user_id,
  combined_opportunities.is_sales_dev_qualified_opportunity,
  IFF(combined_opportunities.is_sales_dev_qualified_opportunity = TRUE, 
      combined_opportunities.dim_crm_opportunity_id, NULL)                                                                                AS sales_accepted_opportunity_id,
  IFF(combined_opportunities.is_net_arr_pipeline_created = 1,
      combined_opportunities.dim_crm_opportunity_id, NULL)                                                                                AS pipeline_opportunity_id,
  CASE
    WHEN combined_opportunities.crm_sales_dev_rep_id IS NOT NULL
      AND combined_opportunities.is_sales_dev_qualified_opportunity = TRUE
      THEN combined_opportunities.dim_crm_opportunity_id
  END                                                                                                                                     AS sdr_sao_id,
  CASE
    WHEN combined_opportunities.crm_business_dev_rep_id IS NOT NULL
      AND combined_opportunities.new_logo_count = 1
      AND combined_opportunities.is_sales_dev_qualified_opportunity = TRUE
      THEN combined_opportunities.dim_crm_opportunity_id
  END                                                                                                                                     AS bdr_first_order_sao_id,
  CASE
    WHEN is_sdr_first_order_booked_deal <> 0
      THEN combined_opportunities.dim_crm_opportunity_id
  END                                                                                                                                     AS sdr_fo_booked_opportunity_id,
  CASE
    WHEN combined_opportunities.crm_business_dev_rep_id IS NOT NULL
      AND combined_opportunities.is_sales_dev_qualified_opportunity = TRUE
      THEN combined_opportunities.net_arr_stage_1
  END                                                                                                                                     AS bdr_stage_1_net_arr,
  CASE
    WHEN combined_opportunities.crm_business_dev_rep_id IS NOT NULL
      AND combined_opportunities.is_sales_dev_qualified_opportunity = TRUE
      THEN combined_opportunities.xdr_net_arr_stage_3
  END                                                                                                                                     AS bdr_stage_3_net_arr,
  CASE
    WHEN combined_opportunities.is_net_arr_pipeline_created = 1
      THEN combined_opportunities.net_arr
    ELSE 0
  END                                                                                                                                     AS pipeline_net_arr,
  CASE
    WHEN combined_opportunities.new_logo_count <> 0 
      AND opp_row_num = 1 
      AND combined_opportunities.is_booked_net_arr = TRUE
      THEN combined_opportunities.net_arr
  END                                                                                                                                     AS first_order_booked_net_arr,
  IFF(opp_row_num = 1, combined_opportunities.net_arr_live, 0)                                                                            AS net_arr_live,
  combined_opportunities.net_arr_stage_1,
  combined_opportunities.xdr_net_arr_stage_1,
  combined_opportunities.xdr_net_arr_stage_3
FROM combined_opportunities
CROSS JOIN current_date_values
LEFT JOIN dim_date AS stage_1_date
  ON combined_opportunities.stage_1_discovery_date = stage_1_date.date_day
LEFT JOIN dim_date AS pipeline_date
  ON combined_opportunities.pipeline_created_date = pipeline_date.date_day
LEFT JOIN dim_date AS close_date
  ON combined_opportunities.close_date = close_date.date_day
LEFT JOIN mart_crm_account
  ON combined_opportunities.dim_crm_account_id = mart_crm_account.dim_crm_account_id