{{ config(
    materialized="table"
) }}

{{ simple_cte([
    ('sheetload_fy_mgp_targets','sheetload_fy_mgp_targets'),
    ('mart_sales_funnel_target','mart_sales_funnel_target'),
    ('dim_date','dim_date')
]) }}

, pipeline_targets AS (

  SELECT
    fiscal_quarter_name_fy,
    CASE 
      WHEN fiscal_quarter_name_fy < current_fiscal_quarter_name_fy 
      THEN 1
      WHEN fiscal_quarter_name_fy = current_fiscal_quarter_name_fy 
      THEN (dim_date.current_date_actual - dim_date.first_day_of_fiscal_quarter) / 
      (dim_date.last_day_of_fiscal_quarter - dim_date.first_day_of_fiscal_quarter)
    ELSE 0 END AS pct_qtr_done,
    order_type_name,
    sales_qualified_source_name,
    crm_user_geo as report_geo,
    crm_user_region as report_region,
    mart_sales_funnel_target.crm_user_role_level_1 as report_role_level_1,
    mart_sales_funnel_target.crm_user_role_level_2 as report_role_level_2,
    SUM(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN allocated_target END)  AS pipeline_target
  FROM mart_sales_funnel_target
  INNER JOIN dim_date
    ON mart_sales_funnel_target.target_month = dim_date.first_day_of_month
      AND day_of_month = 1
  WHERE fiscal_quarter_name_fy IN ('FY26-Q1', 'FY26-Q2', 'FY26-Q3', 'FY26-Q4')
  {{dbt_utils.group_by(n=8)}}


), final AS (

  SELECT
    sheetload_fy_mgp_targets.role_level_1,
    sheetload_fy_mgp_targets.role_level_2,
    sheetload_fy_mgp_targets.geo,
    sheetload_fy_mgp_targets.region,
    sheetload_fy_mgp_targets.fiscal_quarter,
    sheetload_fy_mgp_targets.fiscal_quarter_name_fy,
    sheetload_fy_mgp_targets.order_type,
    sheetload_fy_mgp_targets.sales_qualified_source_name,
    sheetload_fy_mgp_targets.include_in_target_attainment,
    pipeline_targets.pct_qtr_done,
    SUM(sheetload_fy_mgp_targets.mgp_contribution * pipeline_targets.pipeline_target) AS marketing_generated_pipeline_target
  FROM pipeline_targets
  LEFT JOIN sheetload_fy_mgp_targets
    ON pipeline_targets.report_role_level_1=sheetload_fy_mgp_targets.role_level_1
      AND pipeline_targets.report_role_level_2=sheetload_fy_mgp_targets.role_level_2
      AND pipeline_targets.order_type_name=sheetload_fy_mgp_targets.order_type
      AND pipeline_targets.sales_qualified_source_name=sheetload_fy_mgp_targets.sales_qualified_source_name
      AND pipeline_targets.report_geo=sheetload_fy_mgp_targets.geo
      AND pipeline_targets.report_region=sheetload_fy_mgp_targets.region
      AND pipeline_targets.fiscal_quarter_name_fy=sheetload_fy_mgp_targets.fiscal_quarter_name_fy
  {{dbt_utils.group_by(n=10)}}

)

SELECT *
FROM final