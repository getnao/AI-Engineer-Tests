{{ simple_cte([
    ('rpt_base_prospects','rpt_base_prospects'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('fct_crm_opportunity', 'fct_crm_opportunity'),
]) }},

final AS (
SELECT
  fct_crm_opportunity.dim_crm_current_account_set_hierarchy_sk,
  fct_crm_opportunity.dim_order_type_id,
  fct_crm_opportunity.dim_sales_qualified_source_id,
  fct_crm_opportunity.close_date,
  SUM(fct_crm_opportunity.new_logo_count) AS new_logo_count,
  SUM(fct_crm_opportunity.net_arr) AS net_arr
FROM rpt_base_prospects AS rpt_base_prospects -- prod.RESTRICTED_SAFE_COMMON_MART_SALES.RPT_BASE_PROSPECTS AS bp
LEFT JOIN fct_crm_opportunity AS fct_crm_opportunity --prod.RESTRICTED_SAFE_COMMON.fct_CRM_OPPORTUNITY AS fact
ON rpt_base_prospects.dim_crm_opportunity_id = fct_crm_opportunity.dim_crm_opportunity_id
LEFT JOIN mart_crm_opportunity AS mart --prod.RESTRICTED_SAFE_COMMON_mart_sales.mart_CRM_OPPORTUNITY AS mart
ON rpt_base_prospects.dim_crm_opportunity_id = mart.dim_crm_opportunity_id
WHERE rpt_base_prospects.is_mid_market_plus = TRUE
AND mart.fpa_master_bookings_flag = TRUE
GROUP BY 1,2,3,4)

SELECT *
FROM final