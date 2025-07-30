{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('mart_crm_opportunity_daily_snapshot', 'mart_crm_opportunity_daily_snapshot'),
    ('mart_crm_opportunity', 'mart_crm_opportunity')
]) }},

third_business_days AS (

  -- Get all third business days with quarter information for both snapshot dates and current quarter logic
  SELECT
    date_day,
    fiscal_year,
    fiscal_quarter,
    fiscal_quarter_name_fy,
    first_day_of_fiscal_quarter,

    -- LAG values for snapshot dates (looking at previous quarter from each 3rd business day)
    LAG(fiscal_year, 1) OVER (ORDER BY date_day)            AS snapshot_fiscal_year,
    LAG(fiscal_quarter, 1) OVER (ORDER BY date_day)         AS snapshot_fiscal_quarter,
    LAG(fiscal_quarter_name_fy, 1) OVER (ORDER BY date_day) AS snapshot_fiscal_quarter_name_fy,

    -- Check if current date has passed this third business day
    IFF(CURRENT_DATE() - 1 >= date_day, TRUE, FALSE)                           AS is_past_third_business_day,

    LAG(fiscal_quarter_name_fy, 1) OVER (ORDER BY first_day_of_fiscal_quarter) AS previous_fiscal_quarter_name_fy
  FROM dim_date
  WHERE 
    is_third_business_day_of_fiscal_quarter = 1
    AND fiscal_year >= 2022
    AND date_day <= CURRENT_DATE() + 90
       
),

current_date_values AS (

  -- Get current date values to be used across all rows
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
  WHERE dim_date.date_day = CURRENT_DATE() - 1

),

quarters_to_use_live AS (

  -- Determine which quarters should use live data
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

  -- Filter third business days for snapshot logic
  SELECT
    date_day,
    snapshot_fiscal_year,
    snapshot_fiscal_quarter,
    snapshot_fiscal_quarter_name_fy
  FROM third_business_days
  WHERE date_day <= CURRENT_DATE() - 1
    AND snapshot_fiscal_quarter_name_fy IS NOT NULL

),


historical_opportunities AS (

  -- Historical quarters from snapshot model
  SELECT
    'Snapshot'                      AS data_source,
    snapshot_dates.date_day         AS snapshot_date,
    snapshot_dates.snapshot_fiscal_year,
    snapshot_dates.snapshot_fiscal_quarter_name_fy,

    mart_crm_opportunity_daily_snapshot.dim_crm_account_id,
    mart_crm_opportunity_daily_snapshot.dim_crm_opportunity_id,
    mart_crm_opportunity_daily_snapshot.dim_parent_crm_account_id,
    mart_crm_opportunity_daily_snapshot.dim_parent_crm_opportunity_id,

    mart_crm_opportunity_daily_snapshot.crm_account_name,
    mart_crm_opportunity_daily_snapshot.parent_crm_account_name,
    mart_crm_opportunity_daily_snapshot.opportunity_name,

    mart_crm_opportunity_daily_snapshot.close_date,
    mart_crm_opportunity_daily_snapshot.close_fiscal_quarter_date,
    mart_crm_opportunity_daily_snapshot.close_fiscal_quarter_name,
    mart_crm_opportunity_daily_snapshot.close_fiscal_year,
    mart_crm_opportunity_daily_snapshot.close_month,
    mart_crm_opportunity_daily_snapshot.arr_created_date,
    mart_crm_opportunity_daily_snapshot.arr_created_fiscal_quarter_name,
    mart_crm_opportunity_daily_snapshot.arr_created_fiscal_quarter_date,
    mart_crm_opportunity_daily_snapshot.arr_created_fiscal_year,
    mart_crm_opportunity_daily_snapshot.arr_created_month,
    mart_crm_opportunity_daily_snapshot.created_date,
    mart_crm_opportunity_daily_snapshot.created_fiscal_quarter_date,
    mart_crm_opportunity_daily_snapshot.created_fiscal_quarter_name,
    mart_crm_opportunity_daily_snapshot.created_fiscal_year,
    mart_crm_opportunity_daily_snapshot.created_month,
    mart_crm_opportunity_daily_snapshot.subscription_start_date,
    mart_crm_opportunity_daily_snapshot.subscription_start_fiscal_quarter_name,
    mart_crm_opportunity_daily_snapshot.subscription_start_month,
    mart_crm_opportunity_daily_snapshot.subscription_end_date,
    mart_crm_opportunity_daily_snapshot.subscription_end_fiscal_quarter_name,
    mart_crm_opportunity_daily_snapshot.subscription_end_month,

    mart_crm_opportunity_daily_snapshot.amount,
    mart_crm_opportunity_daily_snapshot.net_arr,
    mart_crm_opportunity_daily_snapshot.booked_net_arr,
    mart_crm_opportunity_daily_snapshot.churned_contraction_net_arr,
    mart_crm_opportunity_daily_snapshot.arr_basis,
    mart_crm_opportunity_daily_snapshot.arr_basis_for_clari,
    mart_crm_opportunity_daily_snapshot.won_arr_basis_for_clari,
    mart_crm_opportunity_daily_snapshot.recurring_amount,
    mart_crm_opportunity_daily_snapshot.other_non_recurring_amount,
    mart_crm_opportunity_daily_snapshot.total_contract_value,
    mart_crm_opportunity_daily_snapshot.true_up_amount,
    mart_crm_opportunity_daily_snapshot.new_logo_count,

    mart_crm_opportunity_daily_snapshot.professional_services_value,
    mart_crm_opportunity_daily_snapshot.proserv_amount,
    mart_crm_opportunity_daily_snapshot.edu_services_value,
    mart_crm_opportunity_daily_snapshot.investment_services_value,

    mart_crm_opportunity_daily_snapshot.is_closed,
    mart_crm_opportunity_daily_snapshot.is_edu_oss,
    mart_crm_opportunity_daily_snapshot.is_ps_opp,
    mart_crm_opportunity_daily_snapshot.is_booked_net_arr,
    mart_crm_opportunity_daily_snapshot.fpa_master_bookings_flag,

    mart_crm_opportunity_daily_snapshot.opportunity_category,
    mart_crm_opportunity_daily_snapshot.opportunity_deal_size,
    mart_crm_opportunity_daily_snapshot.opportunity_owner_user_segment,
    mart_crm_opportunity_daily_snapshot.opportunity_term,
    mart_crm_opportunity_daily_snapshot.stage_name,
    mart_crm_opportunity_daily_snapshot.deal_path_name,
    mart_crm_opportunity_daily_snapshot.subscription_type,
    mart_crm_opportunity_daily_snapshot.order_type,
    mart_crm_opportunity_daily_snapshot.order_type_grouped,
    mart_crm_opportunity_daily_snapshot.product_category,
    mart_crm_opportunity_daily_snapshot.product_details,
    mart_crm_opportunity_daily_snapshot.sales_qualified_source_grouped,
    mart_crm_opportunity_daily_snapshot.sales_qualified_source_name,
    mart_crm_opportunity_daily_snapshot.sales_path,

    mart_crm_opportunity_daily_snapshot.parent_crm_account_geo,
    mart_crm_opportunity_daily_snapshot.parent_crm_account_geo_pubsec_segment,
    mart_crm_opportunity_daily_snapshot.parent_crm_account_region,
    mart_crm_opportunity_daily_snapshot.parent_crm_account_sales_segment,
    mart_crm_opportunity_daily_snapshot.parent_crm_account_upa_country,
    mart_crm_opportunity_daily_snapshot.parent_crm_account_max_family_employee,

    -- Fields that are always from the Live Opp are renamed to include _live suffix to avoid confusion
    mart_crm_opportunity.report_area                    AS report_area_live,
    mart_crm_opportunity.report_geo                     AS report_geo_live,
    mart_crm_opportunity.report_geo_pubsec_segment      AS report_geo_pubsec_segment_live,
    mart_crm_opportunity.report_region                  AS report_region_live,
    mart_crm_opportunity.report_segment                 AS report_segment_live,
    mart_crm_opportunity.report_role_level_1            AS report_role_level_1_live,
    mart_crm_opportunity.report_role_level_2            AS report_role_level_2_live,
    mart_crm_opportunity.report_role_level_3            AS report_role_level_3_live,
    mart_crm_opportunity.report_role_level_4            AS report_role_level_4_live,
    mart_crm_opportunity.report_role_level_5            AS report_role_level_5_live,
    mart_crm_opportunity.is_mid_market_plus             AS is_mid_market_plus_live,

    mart_crm_opportunity_daily_snapshot.partner_account,
    mart_crm_opportunity_daily_snapshot.partner_account_name,
    mart_crm_opportunity_daily_snapshot.resale_partner_name,
    mart_crm_opportunity_daily_snapshot.partner_discount,
    mart_crm_opportunity_daily_snapshot.partner_discount_calc,
    mart_crm_opportunity_daily_snapshot.aggregate_partner,
    mart_crm_opportunity_daily_snapshot.alliance_type_name,
    mart_crm_opportunity_daily_snapshot.alliance_type_short_name,
    mart_crm_opportunity_daily_snapshot.distributor,
    mart_crm_opportunity_daily_snapshot.reason_for_loss,
    mart_crm_opportunity_daily_snapshot.reason_for_loss_details,
    mart_crm_opportunity_daily_snapshot.competitors,
    mart_crm_opportunity_daily_snapshot.invoice_number

  FROM mart_crm_opportunity_daily_snapshot
  INNER JOIN snapshot_dates
    ON mart_crm_opportunity_daily_snapshot.snapshot_date = snapshot_dates.date_day
  LEFT JOIN mart_crm_opportunity
    ON mart_crm_opportunity_daily_snapshot.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN quarters_to_use_live
    ON mart_crm_opportunity_daily_snapshot.close_fiscal_quarter_name = quarters_to_use_live.fiscal_quarter_name_fy
  WHERE snapshot_dates.snapshot_fiscal_quarter_name_fy = mart_crm_opportunity_daily_snapshot.close_fiscal_quarter_name
    -- Exclude quarters that should use live data
    AND quarters_to_use_live.fiscal_quarter_name_fy IS NULL 

),

current_quarter_opportunities AS (

  SELECT
    'Live'               AS data_source,
    NULL                 AS snapshot_date,
    NULL                 AS snapshot_fiscal_year,
    NULL                 AS snapshot_fiscal_quarter_name_fy,

    mart_crm_opportunity.dim_crm_account_id,
    mart_crm_opportunity.dim_crm_opportunity_id,
    mart_crm_opportunity.dim_parent_crm_account_id,
    mart_crm_opportunity.dim_parent_crm_opportunity_id,

    mart_crm_opportunity.crm_account_name,
    mart_crm_opportunity.parent_crm_account_name,
    mart_crm_opportunity.opportunity_name,

    mart_crm_opportunity.close_date,
    mart_crm_opportunity.close_fiscal_quarter_date,
    mart_crm_opportunity.close_fiscal_quarter_name,
    mart_crm_opportunity.close_fiscal_year,
    mart_crm_opportunity.close_month,
    mart_crm_opportunity.arr_created_date,
    mart_crm_opportunity.arr_created_fiscal_quarter_name,
    mart_crm_opportunity.arr_created_fiscal_quarter_date,
    mart_crm_opportunity.arr_created_fiscal_year,
    mart_crm_opportunity.arr_created_month,
    mart_crm_opportunity.created_date,
    mart_crm_opportunity.created_fiscal_quarter_date,
    mart_crm_opportunity.created_fiscal_quarter_name,
    mart_crm_opportunity.created_fiscal_year,
    mart_crm_opportunity.created_month,
    mart_crm_opportunity.subscription_start_date,
    mart_crm_opportunity.subscription_start_fiscal_quarter_name,
    mart_crm_opportunity.subscription_start_month,
    mart_crm_opportunity.subscription_end_date,
    mart_crm_opportunity.subscription_end_fiscal_quarter_name,
    mart_crm_opportunity.subscription_end_month,

    mart_crm_opportunity.amount,
    mart_crm_opportunity.net_arr,
    mart_crm_opportunity.booked_net_arr,
    mart_crm_opportunity.churned_contraction_net_arr,
    mart_crm_opportunity.arr_basis,
    mart_crm_opportunity.arr_basis_for_clari,
    mart_crm_opportunity.won_arr_basis_for_clari,
    mart_crm_opportunity.recurring_amount,
    mart_crm_opportunity.other_non_recurring_amount,
    mart_crm_opportunity.total_contract_value,
    mart_crm_opportunity.true_up_amount,
    mart_crm_opportunity.new_logo_count,

    mart_crm_opportunity.professional_services_value,
    mart_crm_opportunity.proserv_amount,
    mart_crm_opportunity.edu_services_value,
    mart_crm_opportunity.investment_services_value,

    mart_crm_opportunity.is_closed,
    mart_crm_opportunity.is_edu_oss,
    mart_crm_opportunity.is_ps_opp,
    mart_crm_opportunity.is_booked_net_arr,
    mart_crm_opportunity.fpa_master_bookings_flag,

    mart_crm_opportunity.opportunity_category,
    mart_crm_opportunity.opportunity_deal_size,
    mart_crm_opportunity.opportunity_owner_user_segment,
    mart_crm_opportunity.opportunity_term,
    mart_crm_opportunity.stage_name,
    mart_crm_opportunity.deal_path_name,
    mart_crm_opportunity.subscription_type,
    mart_crm_opportunity.order_type,
    mart_crm_opportunity.order_type_grouped,
    mart_crm_opportunity.product_category,
    mart_crm_opportunity.product_details,
    mart_crm_opportunity.sales_qualified_source_grouped,
    mart_crm_opportunity.sales_qualified_source_name,
    mart_crm_opportunity.sales_path,

    mart_crm_opportunity.parent_crm_account_geo,
    mart_crm_opportunity.parent_crm_account_geo_pubsec_segment,
    mart_crm_opportunity.parent_crm_account_region,
    mart_crm_opportunity.parent_crm_account_sales_segment,
    mart_crm_opportunity.parent_crm_account_upa_country,
    mart_crm_opportunity.parent_crm_account_max_family_employee,

    -- Fields that are always from the Live Opp are renamed to include _live suffix to avoid confusion
    mart_crm_opportunity.report_area                    AS report_area_live,
    mart_crm_opportunity.report_geo                     AS report_geo_live,
    mart_crm_opportunity.report_geo_pubsec_segment      AS report_geo_pubsec_segment_live,
    mart_crm_opportunity.report_region                  AS report_region_live,
    mart_crm_opportunity.report_segment                 AS report_segment_live,
    mart_crm_opportunity.report_role_level_1            AS report_role_level_1_live,
    mart_crm_opportunity.report_role_level_2            AS report_role_level_2_live,
    mart_crm_opportunity.report_role_level_3            AS report_role_level_3_live,
    mart_crm_opportunity.report_role_level_4            AS report_role_level_4_live,
    mart_crm_opportunity.report_role_level_5            AS report_role_level_5_live,
    mart_crm_opportunity.is_mid_market_plus             AS is_mid_market_plus_live,

    mart_crm_opportunity.partner_account,
    mart_crm_opportunity.partner_account_name,
    mart_crm_opportunity.resale_partner_name,
    mart_crm_opportunity.partner_discount,
    mart_crm_opportunity.partner_discount_calc,
    mart_crm_opportunity.aggregate_partner,
    mart_crm_opportunity.alliance_type_name,
    mart_crm_opportunity.alliance_type_short_name,
    mart_crm_opportunity.distributor,
    mart_crm_opportunity.reason_for_loss,
    mart_crm_opportunity.reason_for_loss_details,
    mart_crm_opportunity.competitors,
    mart_crm_opportunity.invoice_number

  FROM mart_crm_opportunity
  INNER JOIN quarters_to_use_live 
    ON mart_crm_opportunity.close_fiscal_quarter_name = quarters_to_use_live.fiscal_quarter_name_fy

),

combined_opportunities AS (
  
  SELECT * FROM historical_opportunities

  UNION ALL

  SELECT * FROM current_quarter_opportunities

),

final AS (

  SELECT *
  FROM combined_opportunities
  CROSS JOIN current_date_values

)

SELECT * FROM final