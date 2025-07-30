{{ config(materialized='table') }}
{% set min_date = "TO_DATE('2023-02-01')" %} -- earliest date required for reporting in this table

{{ simple_cte([
    ('mart_crm_attribution_touchpoint','mart_crm_attribution_touchpoint'),
    ('rpt_crm_opportunity_pipeline_snapshot','rpt_crm_opportunity_pipeline_snapshot'),
    ('mart_crm_opportunity','mart_crm_opportunity'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_date','dim_date')
]) }}

, opportunity_base AS (
  -- Enrich pipeline snapshot with fields from mart_crm_opportunity and dim_crm_account
  SELECT
    rpt_crm_opportunity_pipeline_snapshot.*,
    
    -- Account fields from dim_crm_account
    dim_crm_account.dim_parent_crm_account_id,
    dim_crm_account.crm_account_name,
    dim_crm_account.parent_crm_account_name,
    
    -- Opportunity attributes from mart_crm_opportunity
    mart_crm_opportunity.subscription_type,
    
    -- Opportunity flags from mart_crm_opportunity
    mart_crm_opportunity.is_web_portal_purchase,
    mart_crm_opportunity.is_edu_oss,
    mart_crm_opportunity.is_open,
    mart_crm_opportunity.is_lost,
    mart_crm_opportunity.is_renewal,
    mart_crm_opportunity.is_refund,
    mart_crm_opportunity.is_credit,
    
    -- Renamed fields to match original naming convention
    rpt_crm_opportunity_pipeline_snapshot.pipeline_day_of_fiscal_quarter AS pipeline_created_day_of_fiscal_quarter_normalised,
    rpt_crm_opportunity_pipeline_snapshot.pipeline_day_of_fiscal_year AS pipeline_created_day_of_fiscal_year_normalised,
    rpt_crm_opportunity_pipeline_snapshot.current_day_of_fiscal_quarter AS current_day_of_fiscal_quarter_normalised,
    
    -- Snapshot date (using the snapshot quarter date as proxy for now)
    rpt_crm_opportunity_pipeline_snapshot.snapshot_fiscal_quarter_date AS opportunity_snapshot_date
    
  FROM rpt_crm_opportunity_pipeline_snapshot
  LEFT JOIN mart_crm_opportunity
    ON rpt_crm_opportunity_pipeline_snapshot.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN dim_crm_account
    ON rpt_crm_opportunity_pipeline_snapshot.dim_crm_account_id = dim_crm_account.dim_crm_account_id
  WHERE rpt_crm_opportunity_pipeline_snapshot.pipeline_created_date >= {{ min_date }}
)

, final AS (
  SELECT 
    -- IDs
    opportunity_base.dim_crm_opportunity_id,
    opportunity_base.dim_crm_account_id,
    opportunity_base.dim_parent_crm_account_id,
    touchpoint.dim_crm_touchpoint_id,

    -- Dates
    opportunity_base.created_date,
    opportunity_base.sales_accepted_date,
    opportunity_base.pipeline_created_date,
    opportunity_base.pipeline_created_fiscal_quarter_name,
    opportunity_base.pipeline_created_fiscal_year,
    opportunity_base.pipeline_created_day_of_fiscal_quarter_normalised,
    opportunity_base.pipeline_created_day_of_fiscal_year_normalised,
    opportunity_base.current_day_of_fiscal_quarter_normalised,
    opportunity_base.close_date,
    opportunity_base.close_fiscal_quarter_name,
    touchpoint.bizible_touchpoint_date,
    opportunity_base.opportunity_snapshot_date,

    -- Account Info
    opportunity_base.parent_crm_account_sales_segment,
    opportunity_base.parent_crm_account_geo,
    opportunity_base.parent_crm_account_region,
    opportunity_base.parent_crm_account_area,
    opportunity_base.crm_account_name AS account_name,
    opportunity_base.parent_crm_account_name,

    -- Opportunity Dimensions
    opportunity_base.new_logo_count,
    opportunity_base.new_logo_count_snapshot,
    opportunity_base.opportunity_category,
    opportunity_base.subscription_type,
    opportunity_base.order_type,
    opportunity_base.order_type_target_match,
    opportunity_base.sales_qualified_source_name,
    opportunity_base.sdr_sqs_or_not,
    opportunity_base.stage_name,
    opportunity_base.report_segment,
    opportunity_base.report_geo,
    opportunity_base.report_area,
    opportunity_base.report_region,
    opportunity_base.parent_crm_account_geo_pubsec_segment,
    opportunity_base.report_role_level_1,
    opportunity_base.report_role_level_2,
    opportunity_base.report_role_level_3,
    opportunity_base.pipe_council_grouping,

    -- Touchpoint Dimensions
    touchpoint.bizible_touchpoint_type, 
    touchpoint.bizible_integrated_campaign_grouping,
    touchpoint.touchpoint_sales_stage AS opp_touchpoint_sales_stage,
    touchpoint.bizible_marketing_channel,
    touchpoint.bizible_marketing_channel_path,
    touchpoint.marketing_review_channel_grouping,
    touchpoint.bizible_ad_campaign_name,
    touchpoint.bizible_form_url,
    touchpoint.budget_holder,
    touchpoint.campaign_rep_role_name,
    touchpoint.campaign_region,
    touchpoint.campaign_sub_region,
    touchpoint.budgeted_cost,
    touchpoint.actual_cost,
    touchpoint.utm_campaign,
    touchpoint.utm_source,
    touchpoint.utm_medium,
    touchpoint.utm_content,
    touchpoint.utm_budget,
    touchpoint.utm_allptnr,
    touchpoint.utm_partnerid,
    touchpoint.devrel_campaign_type,
    touchpoint.devrel_campaign_description,
    touchpoint.devrel_campaign_influence_type,
    touchpoint.integrated_budget_holder,
    touchpoint.type AS sfdc_campaign_type,
    touchpoint.gtm_motion,
    touchpoint.account_demographics_sales_segment AS person_sales_segment,
    touchpoint.touchpoint_offer_type,
    touchpoint.touchpoint_offer_type_grouped,
    touchpoint.is_mgp_opportunity,
    touchpoint.is_mgp_channel_based,

    -- Model Weights
    touchpoint.bizible_count_custom_model AS custom_model_weight,
    touchpoint.gitlab_model_weight,
    touchpoint.time_decay_model_weight,
    touchpoint.data_driven_model_weight,
    
    -- Metrics
    opportunity_base.pipeline_net_arr,
    NULL::NUMBER AS pipeline_net_arr_qtd, 

    COALESCE(
      touchpoint.bizible_count_custom_model, 
      CASE 
        WHEN opportunity_base.sales_qualified_source_name IN ('SDR Generated', 'Web Direct Generated')
          THEN 1
        ELSE 0
      END
    ) AS bizible_count_custom_model,
    
    opportunity_base.pipeline_net_arr * bizible_count_custom_model AS custom_net_arr,
    opportunity_base.pipeline_net_arr * COALESCE(touchpoint.gitlab_model_weight, 0) AS gitlab_model_net_arr,
    opportunity_base.pipeline_net_arr * COALESCE(touchpoint.time_decay_model_weight, 0) AS time_decay_model_net_arr,
    opportunity_base.pipeline_net_arr * COALESCE(touchpoint.data_driven_model_weight, 0) AS data_driven_model_net_arr,
    
    -- Nullified QTD metrics until deprecated in Tableau
    NULL::NUMBER AS custom_net_arr_qtd,
    NULL::NUMBER AS gitlab_model_net_arr_qtd,
    NULL::NUMBER AS time_decay_model_net_arr_qtd,
    NULL::NUMBER AS data_driven_model_net_arr_qtd,
    
    -- Flags
    opportunity_base.is_sao,
    opportunity_base.is_won,
    opportunity_base.is_web_portal_purchase,
    opportunity_base.fpa_master_bookings_flag,
    opportunity_base.is_edu_oss,
    opportunity_base.is_net_arr_pipeline_created AS is_eligible_created_pipeline_flag,
    opportunity_base.is_net_arr_pipeline_created,
    opportunity_base.is_open,
    opportunity_base.is_lost,
    opportunity_base.is_closed,
    opportunity_base.is_renewal,
    opportunity_base.is_refund,
    opportunity_base.is_credit AS is_credit_flag,
    opportunity_base.is_eligible_open_pipeline AS is_eligible_open_pipeline_flag,
    opportunity_base.is_booked_net_arr AS is_booked_net_arr_flag,
    opportunity_base.is_eligible_age_analysis AS is_eligible_age_analysis_flag,
    opportunity_base.is_sales_dev_pipeline_created

  FROM opportunity_base
  LEFT JOIN mart_crm_attribution_touchpoint AS touchpoint
    ON opportunity_base.dim_crm_opportunity_id = touchpoint.dim_crm_opportunity_id
)

SELECT *
FROM final