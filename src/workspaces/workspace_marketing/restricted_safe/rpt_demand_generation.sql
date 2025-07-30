{{ config(materialized='table') }}
{% set min_date = "'2023-02-01'" %}  -- earliest date required for reporting in this table

{{ simple_cte([
    ('mart_crm_person', 'mart_crm_person'),
    ('mart_crm_touchpoint', 'mart_crm_touchpoint'),
    ('mart_crm_attribution_touchpoint', 'mart_crm_attribution_touchpoint'),
    ('mart_crm_account', 'mart_crm_account'),
    ('dim_campaign', 'dim_campaign'),
    ('fct_campaign', 'fct_campaign'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('rpt_crm_opportunity_pipeline_snapshot', 'rpt_crm_opportunity_pipeline_snapshot'),
    ('dim_date', 'dim_date'),
    ('dim_crm_user', 'dim_crm_user'),
    ('rpt_sales_dev_activity', 'rpt_sales_dev_activity'),
    ('map_person_territory', 'map_person_territory')
]) }}

, single_row_date_helper AS (
  -- joined on the last CTE as a cross join so every row has this field.
  SELECT
    current_day_of_fiscal_quarter_normalised - 1 AS current_day_of_fiscal_quarter_normalised
  FROM dim_date
  WHERE date_actual = current_date_actual
)

, previous_quarter_date AS (
  SELECT
    fiscal_quarter_name_fy,
    current_fiscal_quarter_name_fy,
    LAG(fiscal_quarter_name_fy, 1) OVER (ORDER BY date_day) AS previous_fiscal_quarter_name_fy,
    LAG(fiscal_quarter_name_fy, 4) OVER (ORDER BY date_day) AS previous_fiscal_year_quarter_name_fy
  FROM dim_date
  WHERE is_third_business_day_of_fiscal_quarter
    AND date_day <= CURRENT_DATE
    AND fiscal_year >= 2015
  QUALIFY fiscal_year > 2016
)

, rpt_sales_dev_activity_worked AS (
  -- This isn't ideal, but our worked logic is on this report right now.
  -- This CTE gets the worked state of people who have been worked to join to the final CTE below.
  SELECT
    worked_mql_person_id,
    MAX(activity_date) AS last_activity_date
  FROM rpt_sales_dev_activity
  WHERE worked_mql_person_id IS NOT NULL
  GROUP BY ALL
)

, mart_crm_attribution_touchpoint_filtered AS (
  SELECT 
    *
  FROM mart_crm_attribution_touchpoint
  WHERE gitlab_model_weight != 0 
    AND pipeline_created_date >= {{ min_date }}
)

, opportunity_base AS (
  SELECT
    rpt_crm_opportunity_pipeline_snapshot.dim_crm_opportunity_id, 
    rpt_crm_opportunity_pipeline_snapshot.dim_crm_account_id,
    rpt_crm_opportunity_pipeline_snapshot.order_type,
    rpt_crm_opportunity_pipeline_snapshot.order_type_target_match AS order_type_grouped,
    rpt_crm_opportunity_pipeline_snapshot.sales_accepted_date,
    rpt_crm_opportunity_pipeline_snapshot.sales_qualified_source_name,
    mart_crm_opportunity.sdr_or_bdr,
    rpt_crm_opportunity_pipeline_snapshot.stage_name,
    rpt_crm_opportunity_pipeline_snapshot.close_date,
    rpt_crm_opportunity_pipeline_snapshot.close_fiscal_quarter_name,
    rpt_crm_opportunity_pipeline_snapshot.created_date,
    mart_crm_opportunity.subscription_type,
    mart_crm_opportunity.forecast_category_name,
    mart_crm_opportunity.days_in_stage,
    rpt_crm_opportunity_pipeline_snapshot.fpa_master_bookings_flag,
    rpt_crm_opportunity_pipeline_snapshot.is_booked_net_arr,
    rpt_crm_opportunity_pipeline_snapshot.is_closed,
    mart_crm_opportunity.is_credit,
    mart_crm_opportunity.is_edu_oss,
    rpt_crm_opportunity_pipeline_snapshot.is_eligible_age_analysis,
    rpt_crm_opportunity_pipeline_snapshot.is_net_arr_pipeline_created,
    rpt_crm_opportunity_pipeline_snapshot.is_eligible_open_pipeline,
    mart_crm_opportunity.is_lost,
    mart_crm_opportunity.is_open,
    rpt_crm_opportunity_pipeline_snapshot.new_logo_count,
    rpt_crm_opportunity_pipeline_snapshot.new_logo_count_snapshot,
    mart_crm_opportunity.is_refund,
    mart_crm_opportunity.is_registration_from_portal,
    mart_crm_opportunity.is_renewal,
    rpt_crm_opportunity_pipeline_snapshot.is_sao,
    mart_crm_opportunity.is_web_portal_purchase,
    rpt_crm_opportunity_pipeline_snapshot.is_won,
    rpt_crm_opportunity_pipeline_snapshot.opportunity_category,
    mart_crm_opportunity.opportunity_name,
    rpt_crm_opportunity_pipeline_snapshot.parent_crm_account_geo_pubsec_segment,
    rpt_crm_opportunity_pipeline_snapshot.pipe_council_grouping,
    rpt_crm_opportunity_pipeline_snapshot.pipeline_created_date,
    rpt_crm_opportunity_pipeline_snapshot.pipeline_created_fiscal_quarter_name,
    rpt_crm_opportunity_pipeline_snapshot.pipeline_created_fiscal_year,
    rpt_crm_opportunity_pipeline_snapshot.pipeline_day_of_fiscal_quarter     AS day_of_fiscal_quarter_normalised,
    rpt_crm_opportunity_pipeline_snapshot.pipeline_day_of_fiscal_year        AS day_of_fiscal_year_normalised,
    rpt_crm_opportunity_pipeline_snapshot.current_day_of_fiscal_quarter      AS current_day_of_fiscal_quarter_normalised,
    rpt_crm_opportunity_pipeline_snapshot.report_area,
    rpt_crm_opportunity_pipeline_snapshot.report_geo,
    rpt_crm_opportunity_pipeline_snapshot.report_region,
    rpt_crm_opportunity_pipeline_snapshot.report_role_level_1,
    rpt_crm_opportunity_pipeline_snapshot.report_role_level_2,
    rpt_crm_opportunity_pipeline_snapshot.report_role_level_3,
    rpt_crm_opportunity_pipeline_snapshot.report_segment,
    rpt_crm_opportunity_pipeline_snapshot.sdr_sqs_or_not,
    rpt_crm_opportunity_pipeline_snapshot.order_type_target_match,
    rpt_crm_opportunity_pipeline_snapshot.pipeline_net_arr,
    NULL::NUMBER                                                             AS pipeline_net_arr_qtd, -- Nullified as requested
    rpt_crm_opportunity_pipeline_snapshot.net_arr_live                      AS net_arr
  FROM rpt_crm_opportunity_pipeline_snapshot
  LEFT JOIN mart_crm_opportunity
    ON rpt_crm_opportunity_pipeline_snapshot.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
  WHERE rpt_crm_opportunity_pipeline_snapshot.pipeline_created_date >= {{ min_date }}
    AND rpt_crm_opportunity_pipeline_snapshot.is_net_arr_pipeline_created = 1
)

, touchpoint_base AS (
  -- First part: All touchpoints with their attribution data when available
  SELECT 
    mart_crm_touchpoint.dim_campaign_id,
    mart_crm_touchpoint.dim_crm_account_id,
    mart_crm_touchpoint.dim_crm_person_id,
    mart_crm_touchpoint.dim_crm_touchpoint_id,
    mart_crm_attribution_touchpoint_filtered.dim_crm_touchpoint_id AS dim_crm_attribution_touchpoint_id, 
    mart_crm_touchpoint.dim_parent_crm_account_id,
    mart_crm_touchpoint.sfdc_record_id,
    dim_campaign.series_campaign_id,
    fct_campaign.dim_parent_campaign_id,
    mart_crm_attribution_touchpoint_filtered.dim_crm_opportunity_id,
    fct_campaign.start_date       AS campaign_start_date,
    parent_campaign.campaign_name AS parent_campaign_name,
    {{ integrated_campaign_type('dim_campaign.type', 'mart_crm_touchpoint.utm_campaign_type') }},
    mart_crm_touchpoint.bizible_touchpoint_date,    
    mart_crm_touchpoint.utm_campaign_date,
    mart_crm_touchpoint.actual_cost,
    mart_crm_touchpoint.alliance_partner_name,
    mart_crm_touchpoint.bizible_ad_campaign_name,
    mart_crm_touchpoint.bizible_form_url,
    mart_crm_touchpoint.bizible_form_url_raw,
    mart_crm_touchpoint.bizible_integrated_campaign_grouping,
    mart_crm_touchpoint.bizible_landing_page,
    mart_crm_touchpoint.bizible_landing_page_raw,
    mart_crm_touchpoint.bizible_marketing_channel,
    mart_crm_touchpoint.bizible_marketing_channel_path,
    mart_crm_touchpoint.bizible_referrer_page,
    mart_crm_touchpoint.bizible_referrer_page_raw,
    mart_crm_touchpoint.bizible_salesforce_campaign,
    mart_crm_touchpoint.bizible_touchpoint_position,
    mart_crm_touchpoint.bizible_touchpoint_type,
    mart_crm_touchpoint.budgeted_cost,
    {{ integrated_campaign_region('mart_crm_touchpoint.campaign_region', 'mart_crm_touchpoint.utm_campaign_region') }},
    mart_crm_touchpoint.campaign_sub_region,
    mart_crm_touchpoint.campaign_rep_name AS campaign_owner,
    campaign_owner_manager.user_name      AS campaign_owner_manager,
    mart_crm_touchpoint.channel_partner_name,
    mart_crm_touchpoint.crm_person_status,
    mart_crm_touchpoint.pre_mql_weight AS mql_sum,
    mart_crm_touchpoint.devrel_campaign_type,
    {{ integrated_gtm('mart_crm_touchpoint.gtm_motion', 'mart_crm_touchpoint.utm_campaign_gtm') }},
    mart_crm_touchpoint.integrated_budget_holder,
    mart_crm_touchpoint.is_a_channel_partner_involved,
    mart_crm_touchpoint.is_an_alliance_partner_involved,
    mart_crm_touchpoint.keystone_content_name,
    mart_crm_touchpoint.keystone_type,
    mart_crm_touchpoint.keystone_url_slug,
    mart_crm_touchpoint.marketing_review_channel_grouping,
    mart_crm_touchpoint.touchpoint_offer_type,
    mart_crm_touchpoint.touchpoint_offer_type_grouped,
    mart_crm_touchpoint.touchpoint_segment,
    mart_crm_touchpoint.utm_allptnr,
    mart_crm_touchpoint.utm_campaign,
    mart_crm_touchpoint.utm_campaign_agency,
    mart_crm_touchpoint.utm_campaign_language,
    mart_crm_touchpoint.utm_campaign_name,
    mart_crm_touchpoint.utm_content,
    mart_crm_touchpoint.utm_content_asset_type,
    mart_crm_touchpoint.utm_content_industry,
    mart_crm_touchpoint.utm_content_offer,
    mart_crm_touchpoint.utm_medium,
    mart_crm_touchpoint.utm_partnerid,
    mart_crm_touchpoint.utm_source,
    mart_crm_attribution_touchpoint_filtered.bizible_count_custom_model,
    mart_crm_attribution_touchpoint_filtered.bizible_weight_first_touch,
    mart_crm_attribution_touchpoint_filtered.data_driven_model_weight,
    mart_crm_attribution_touchpoint_filtered.gitlab_model_weight,
    mart_crm_attribution_touchpoint_filtered.is_mgp_channel_based,
    mart_crm_attribution_touchpoint_filtered.is_mgp_opportunity,
    mart_crm_attribution_touchpoint_filtered.time_decay_model_weight,
    mart_crm_attribution_touchpoint_filtered.touchpoint_sales_stage,
    mart_crm_attribution_touchpoint_filtered.pipeline_created_date 
  FROM mart_crm_touchpoint
  LEFT JOIN mart_crm_attribution_touchpoint_filtered
    ON mart_crm_touchpoint.dim_crm_touchpoint_id = mart_crm_attribution_touchpoint_filtered.dim_crm_buyer_touchpoint_id
  LEFT JOIN dim_campaign
    ON mart_crm_touchpoint.dim_campaign_id = dim_campaign.dim_campaign_id
  LEFT JOIN fct_campaign
    ON dim_campaign.dim_campaign_id = fct_campaign.dim_campaign_id
  LEFT JOIN dim_campaign AS parent_campaign
    ON fct_campaign.dim_parent_campaign_id = parent_campaign.dim_campaign_id
  LEFT JOIN dim_crm_user campaign_owner
    ON fct_campaign.campaign_owner_id = campaign_owner.dim_crm_user_id
  LEFT JOIN dim_crm_user campaign_owner_manager
    ON campaign_owner.manager_id = campaign_owner_manager.dim_crm_user_id
  WHERE  
    mart_crm_touchpoint.bizible_touchpoint_date >= {{ min_date }} OR 
    mart_crm_attribution_touchpoint_filtered.pipeline_created_date >= {{ min_date }}

  UNION ALL

  -- Second part: Attribution touchpoints that don't exist in the main touchpoint table
  SELECT 
    mart_crm_attribution_touchpoint_filtered.dim_campaign_id,
    mart_crm_attribution_touchpoint_filtered.dim_crm_account_id,
    mart_crm_attribution_touchpoint_filtered.dim_crm_person_id,
    NULL AS dim_crm_touchpoint_id, -- This will be NULL for these records
    mart_crm_attribution_touchpoint_filtered.dim_crm_touchpoint_id AS dim_crm_attribution_touchpoint_id,
    mart_crm_attribution_touchpoint_filtered.dim_parent_crm_account_id,
    mart_crm_attribution_touchpoint_filtered.sfdc_record_id,
    dim_campaign.series_campaign_id,
    fct_campaign.dim_parent_campaign_id,
    mart_crm_attribution_touchpoint_filtered.dim_crm_opportunity_id,
    fct_campaign.start_date       AS campaign_start_date,
    parent_campaign.campaign_name AS parent_campaign_name,
    {{ integrated_campaign_type('dim_campaign.type', 'mart_crm_touchpoint.utm_campaign_type') }},
    mart_crm_attribution_touchpoint_filtered.bizible_touchpoint_date,
    NULL AS utm_campaign_date, -- Fields only in mart_crm_touchpoint
    NULL AS actual_cost,
    mart_crm_attribution_touchpoint_filtered.alliance_partner_name,
    mart_crm_attribution_touchpoint_filtered.bizible_ad_campaign_name,
    mart_crm_attribution_touchpoint_filtered.bizible_form_url,
    mart_crm_attribution_touchpoint_filtered.bizible_form_url_raw,
    mart_crm_attribution_touchpoint_filtered.bizible_integrated_campaign_grouping,
    mart_crm_attribution_touchpoint_filtered.bizible_landing_page,
    mart_crm_attribution_touchpoint_filtered.bizible_landing_page_raw,
    mart_crm_attribution_touchpoint_filtered.bizible_marketing_channel,
    mart_crm_attribution_touchpoint_filtered.bizible_marketing_channel_path,
    mart_crm_attribution_touchpoint_filtered.bizible_referrer_page,
    mart_crm_attribution_touchpoint_filtered.bizible_referrer_page_raw,
    mart_crm_attribution_touchpoint_filtered.bizible_salesforce_campaign,
    mart_crm_attribution_touchpoint_filtered.bizible_touchpoint_position,
    mart_crm_attribution_touchpoint_filtered.bizible_touchpoint_type,
    mart_crm_attribution_touchpoint_filtered.budgeted_cost,
    {{ integrated_campaign_region('mart_crm_attribution_touchpoint_filtered.campaign_region', 'mart_crm_attribution_touchpoint_filtered.utm_campaign_region') }},
    mart_crm_attribution_touchpoint_filtered.campaign_sub_region,
    mart_crm_attribution_touchpoint_filtered.campaign_rep_name AS campaign_owner,
    campaign_owner_manager.user_name                           AS campaign_owner_manager,
    mart_crm_attribution_touchpoint_filtered.channel_partner_name,
    mart_crm_attribution_touchpoint_filtered.crm_person_status,
    NULL AS mql_sum,
    mart_crm_attribution_touchpoint_filtered.devrel_campaign_type,
    {{ integrated_gtm('mart_crm_attribution_touchpoint_filtered.gtm_motion', 'mart_crm_attribution_touchpoint_filtered.utm_campaign_gtm') }},
    mart_crm_attribution_touchpoint_filtered.integrated_budget_holder,
    mart_crm_attribution_touchpoint_filtered.is_a_channel_partner_involved,
    mart_crm_attribution_touchpoint_filtered.is_an_alliance_partner_involved,
    mart_crm_attribution_touchpoint_filtered.keystone_content_name,
    mart_crm_attribution_touchpoint_filtered.keystone_type,
    mart_crm_attribution_touchpoint_filtered.keystone_url_slug,
    mart_crm_attribution_touchpoint_filtered.marketing_review_channel_grouping,
    mart_crm_attribution_touchpoint_filtered.touchpoint_offer_type,
    mart_crm_attribution_touchpoint_filtered.touchpoint_offer_type_grouped,
    mart_crm_attribution_touchpoint_filtered.touchpoint_segment,
    mart_crm_attribution_touchpoint_filtered.utm_allptnr,
    mart_crm_attribution_touchpoint_filtered.utm_campaign,
    mart_crm_attribution_touchpoint_filtered.utm_campaign_agency,
    mart_crm_attribution_touchpoint_filtered.utm_campaign_language,
    mart_crm_attribution_touchpoint_filtered.utm_campaign_name,
    mart_crm_attribution_touchpoint_filtered.utm_content,
    mart_crm_attribution_touchpoint_filtered.utm_content_asset_type,
    mart_crm_attribution_touchpoint_filtered.utm_content_industry,
    mart_crm_attribution_touchpoint_filtered.utm_content_offer,
    mart_crm_attribution_touchpoint_filtered.utm_medium,
    mart_crm_attribution_touchpoint_filtered.utm_partnerid,
    mart_crm_attribution_touchpoint_filtered.utm_source,
    mart_crm_attribution_touchpoint_filtered.bizible_count_custom_model,
    mart_crm_attribution_touchpoint_filtered.bizible_weight_first_touch,
    mart_crm_attribution_touchpoint_filtered.data_driven_model_weight,
    mart_crm_attribution_touchpoint_filtered.gitlab_model_weight,
    mart_crm_attribution_touchpoint_filtered.is_mgp_channel_based,
    mart_crm_attribution_touchpoint_filtered.is_mgp_opportunity,
    mart_crm_attribution_touchpoint_filtered.time_decay_model_weight,
    mart_crm_attribution_touchpoint_filtered.touchpoint_sales_stage,
    mart_crm_attribution_touchpoint_filtered.pipeline_created_date 
  FROM mart_crm_attribution_touchpoint_filtered
  LEFT JOIN mart_crm_touchpoint
    ON mart_crm_attribution_touchpoint_filtered.dim_crm_buyer_touchpoint_id = mart_crm_touchpoint.dim_crm_touchpoint_id
  LEFT JOIN dim_campaign
    ON mart_crm_attribution_touchpoint_filtered.dim_campaign_id = dim_campaign.dim_campaign_id
  LEFT JOIN fct_campaign
    ON dim_campaign.dim_campaign_id = fct_campaign.dim_campaign_id
  LEFT JOIN dim_campaign AS parent_campaign
    ON fct_campaign.dim_parent_campaign_id = parent_campaign.dim_campaign_id
  LEFT JOIN dim_crm_user campaign_owner
    ON fct_campaign.campaign_owner_id = campaign_owner.dim_crm_user_id
  LEFT JOIN dim_crm_user campaign_owner_manager
    ON campaign_owner.manager_id = campaign_owner_manager.dim_crm_user_id
  WHERE mart_crm_touchpoint.dim_crm_touchpoint_id IS NULL 
    AND mart_crm_attribution_touchpoint_filtered.pipeline_created_date >= {{ min_date }}
)

, person_touchpoint_combined AS (
  SELECT
    mart_crm_person.dim_crm_person_id,
    mart_crm_person.sfdc_record_id,
    mart_crm_person.dim_crm_account_id,
    mart_crm_person.account_demographics_geo,
    mart_crm_person.account_demographics_region,
    mart_crm_person.account_demographics_sales_segment,
    mart_crm_person.account_demographics_area,
    map_person_territory.report_geo           AS report_person_geo,
    map_person_territory.report_region        AS report_person_region,
    map_person_territory.report_area          AS report_person_area, 
    mart_crm_person.is_exclude_from_reporting AS is_excluded_from_reporting,
    mart_crm_person.is_mql,
    mart_crm_person.is_inquiry,
    mart_crm_person.mql_date_first_pt,
    mart_crm_person.inquiry_date_pt,
    mart_crm_person.email_domain_type,
    mart_crm_person.is_valuable_signup,
    mart_crm_person.lead_score_classification,
    mart_crm_person.partner_prospect_id,
    mart_crm_person.partner_prospect_status,
    mart_crm_person.person_first_country,
    mart_crm_person.is_first_order_initial_mql,
    mart_crm_person.is_first_order_person,
    mart_crm_person.prospect_share_status,
    mart_crm_person.marketo_lead_id,
    mart_crm_person.email_domain,
    mart_crm_person.email_hash,
    mart_crm_person.sfdc_record_type,
    mart_crm_person.lead_source,
    mart_crm_person.source_buckets,
    mart_crm_person.crm_partner_id,
    mart_crm_person.is_partner_recalled,
    mart_crm_person.sdr_sales_segment,
    mart_crm_person.sdr_region,
    mart_crm_person.person_score,
    mart_crm_person.persona_category,
    mart_crm_person.is_management as persona_is_management, 

    -- Inquiry Date
    inq_date.fiscal_quarter_name_fy                               AS inq_fiscal_quarter_name,
    inq_date.day_of_fiscal_quarter_normalised                     AS inq_day_of_fiscal_quarter_normalised,
    inq_date.day_of_fiscal_year_normalised                        AS inq_day_of_fiscal_year_normalised,
    previous_inq_date.previous_fiscal_quarter_name_fy             AS inq_previous_fiscal_quarter_name,
    previous_inq_date.previous_fiscal_year_quarter_name_fy        AS inq_previous_fiscal_year_quarter_name,
    -- MQL Dates
    mql_date.fiscal_quarter_name_fy                               AS mql_fiscal_quarter_name,
    mql_date.day_of_fiscal_quarter_normalised                     AS mql_day_of_fiscal_quarter_normalised,
    mql_date.day_of_fiscal_year_normalised                        AS mql_day_of_fiscal_year_normalised,
    previous_mql_date.previous_fiscal_quarter_name_fy             AS mql_previous_fiscal_quarter_name,
    previous_mql_date.previous_fiscal_year_quarter_name_fy        AS mql_previous_fiscal_year_quarter_name,
    -- Touchpoints Dates
    touchpoint_date.fiscal_quarter_name_fy                        AS touchpoint_fiscal_quarter_name,
    touchpoint_date.day_of_fiscal_quarter_normalised              AS touchpoint_day_of_fiscal_quarter_normalised,
    touchpoint_date.day_of_fiscal_year_normalised                 AS touchpoint_day_of_fiscal_year_normalised,
    previous_touchpoint_date.previous_fiscal_quarter_name_fy      AS touchpoint_previous_fiscal_quarter_name,
    previous_touchpoint_date.previous_fiscal_year_quarter_name_fy AS touchpoint_previous_fiscal_year_quarter_name,

    touchpoint_base.dim_campaign_id,
    touchpoint_base.dim_crm_touchpoint_id,
    touchpoint_base.dim_crm_attribution_touchpoint_id,
    touchpoint_base.dim_parent_crm_account_id,
    touchpoint_base.series_campaign_id,
    touchpoint_base.dim_parent_campaign_id,
    touchpoint_base.dim_crm_opportunity_id,
    touchpoint_base.campaign_start_date,
    touchpoint_base.parent_campaign_name,
    touchpoint_base.integrated_campaign_type,
    touchpoint_base.bizible_touchpoint_date,
    touchpoint_base.utm_campaign_date,
    touchpoint_base.actual_cost,
    touchpoint_base.alliance_partner_name,
    touchpoint_base.bizible_ad_campaign_name,
    touchpoint_base.bizible_form_url,
    touchpoint_base.bizible_form_url_raw,
    touchpoint_base.bizible_integrated_campaign_grouping,
    touchpoint_base.bizible_landing_page,
    touchpoint_base.bizible_landing_page_raw,
    touchpoint_base.bizible_marketing_channel,
    touchpoint_base.bizible_marketing_channel_path,
    touchpoint_base.bizible_referrer_page,
    touchpoint_base.bizible_referrer_page_raw,
    touchpoint_base.bizible_salesforce_campaign,
    touchpoint_base.bizible_touchpoint_position,
    touchpoint_base.bizible_touchpoint_type,
    touchpoint_base.budgeted_cost,
    touchpoint_base.integrated_campaign_region,
    touchpoint_base.campaign_sub_region,
    touchpoint_base.campaign_owner,
    touchpoint_base.campaign_owner_manager,
    touchpoint_base.channel_partner_name,
    touchpoint_base.crm_person_status,
    touchpoint_base.mql_sum,
    touchpoint_base.devrel_campaign_type,
    touchpoint_base.integrated_gtm,
    touchpoint_base.integrated_budget_holder,
    touchpoint_base.is_a_channel_partner_involved,
    touchpoint_base.is_an_alliance_partner_involved,
    touchpoint_base.keystone_content_name,
    touchpoint_base.keystone_type,
    touchpoint_base.keystone_url_slug,
    touchpoint_base.marketing_review_channel_grouping,
    touchpoint_base.touchpoint_offer_type,
    touchpoint_base.touchpoint_offer_type_grouped,
    touchpoint_base.touchpoint_segment,
    touchpoint_base.utm_allptnr,
    touchpoint_base.utm_campaign,
    touchpoint_base.utm_campaign_agency,
    touchpoint_base.utm_campaign_language,
    touchpoint_base.utm_campaign_name,
    touchpoint_base.utm_content,
    touchpoint_base.utm_content_asset_type,
    touchpoint_base.utm_content_industry,
    touchpoint_base.utm_content_offer,
    touchpoint_base.utm_medium,
    touchpoint_base.utm_partnerid,
    touchpoint_base.utm_source,
    touchpoint_base.bizible_count_custom_model,
    touchpoint_base.bizible_weight_first_touch,
    touchpoint_base.data_driven_model_weight,
    touchpoint_base.gitlab_model_weight,
    touchpoint_base.is_mgp_channel_based,
    touchpoint_base.is_mgp_opportunity,
    touchpoint_base.time_decay_model_weight,
    touchpoint_base.touchpoint_sales_stage
  FROM mart_crm_person
  LEFT JOIN map_person_territory
    ON mart_crm_person.dim_crm_person_id = map_person_territory.dim_crm_person_id
  
  LEFT JOIN dim_date mql_date
    ON mart_crm_person.mql_date_first_pt = mql_date.date_day
  LEFT JOIN previous_quarter_date previous_mql_date
    ON previous_mql_date.fiscal_quarter_name_fy = mql_date.fiscal_quarter_name_fy
  
  LEFT JOIN dim_date inq_date
    ON mart_crm_person.inquiry_date_pt = inq_date.date_day
  LEFT JOIN previous_quarter_date previous_inq_date
    ON previous_inq_date.fiscal_quarter_name_fy = inq_date.fiscal_quarter_name_fy
  
  FULL OUTER JOIN touchpoint_base
    ON mart_crm_person.dim_crm_person_id = touchpoint_base.dim_crm_person_id
    AND
    -- Filter to reduce data source size to be accurate from FY24+ 
    (mart_crm_person.inquiry_date_pt >= {{ min_date }} OR 
    mart_crm_person.mql_date_first_pt >= {{ min_date }} )
  
  LEFT JOIN dim_date touchpoint_date
    ON touchpoint_base.bizible_touchpoint_date = touchpoint_date.date_day
  LEFT JOIN previous_quarter_date previous_touchpoint_date
    ON previous_touchpoint_date.fiscal_quarter_name_fy = touchpoint_date.fiscal_quarter_name_fy
  WHERE 1=1
    -- is_exclude_from_reporting = FALSE
)

, final AS (
  SELECT 
    -- Person and touchpoint fields
    person_touchpoint_combined.dim_crm_person_id,
    person_touchpoint_combined.sfdc_record_id,
    IFNULL(opportunity_base.dim_crm_account_id, person_touchpoint_combined.dim_crm_account_id) AS dim_crm_account_id,
    person_touchpoint_combined.dim_crm_touchpoint_id,
    person_touchpoint_combined.dim_crm_attribution_touchpoint_id,
    person_touchpoint_combined.dim_crm_opportunity_id AS pt_dim_crm_opportunity_id,
    mart_crm_account.crm_account_focus_account,
    mart_crm_account.is_base_prospect_account,
    mart_crm_account.is_sdr_target_account,
    mart_crm_account.bdr_prospecting_status,
    mart_crm_account.crm_account_name,
    mart_crm_account.crm_account_type,
    mart_crm_account.dim_parent_crm_account_id,
    mart_crm_account.parent_crm_account_name,
    IFNULL(mart_crm_account.parent_crm_account_geo, person_touchpoint_combined.account_demographics_geo) AS account_demographics_geo, -- To be used for lead / campaign reporting 
    IFNULL(mart_crm_account.parent_crm_account_region, person_touchpoint_combined.account_demographics_region) AS account_demographics_region,
    IFNULL(mart_crm_account.parent_crm_account_area, person_touchpoint_combined.account_demographics_area) AS account_demographics_area,
    IFNULL(mart_crm_account.parent_crm_account_sales_segment, person_touchpoint_combined.account_demographics_sales_segment) AS account_demographics_sales_segment,
    person_touchpoint_combined.report_person_geo,
    person_touchpoint_combined.report_person_region,
    person_touchpoint_combined.report_person_area,
    mart_crm_account.parent_crm_account_geo, -- To be used for Account Level Reporting 
    mart_crm_account.parent_crm_account_region,
    mart_crm_account.parent_crm_account_area,
    mart_crm_account.parent_crm_account_sales_segment,
    person_touchpoint_combined.is_excluded_from_reporting,
    person_touchpoint_combined.is_mql,
    person_touchpoint_combined.is_inquiry,
    person_touchpoint_combined.mql_date_first_pt,
    person_touchpoint_combined.inquiry_date_pt,
    person_touchpoint_combined.email_domain_type,
    person_touchpoint_combined.is_valuable_signup,
    person_touchpoint_combined.lead_score_classification,
    person_touchpoint_combined.partner_prospect_id,
    person_touchpoint_combined.partner_prospect_status,
    person_touchpoint_combined.person_first_country,
    person_touchpoint_combined.is_first_order_initial_mql,
    person_touchpoint_combined.is_first_order_person,
    CASE 
      WHEN opportunity_base.order_type = '1. New - First Order' 
        THEN 'First Order'
      WHEN person_touchpoint_combined.is_first_order_person = TRUE AND opportunity_base.order_type IS NULL
        THEN 'First Order'
    ELSE 'Growth' END AS order_type_combined,
    person_touchpoint_combined.marketo_lead_id,
    person_touchpoint_combined.email_domain,
    person_touchpoint_combined.email_hash,
    person_touchpoint_combined.sfdc_record_type,
    person_touchpoint_combined.lead_source,
    person_touchpoint_combined.source_buckets,
    person_touchpoint_combined.crm_partner_id,
    person_touchpoint_combined.sdr_sales_segment,
    person_touchpoint_combined.sdr_region,
    person_touchpoint_combined.person_score,
    person_touchpoint_combined.dim_campaign_id,
    person_touchpoint_combined.series_campaign_id,
    person_touchpoint_combined.dim_parent_campaign_id,
    person_touchpoint_combined.parent_campaign_name,
    person_touchpoint_combined.campaign_start_date,
    person_touchpoint_combined.integrated_campaign_type,
    person_touchpoint_combined.bizible_touchpoint_date,
    person_touchpoint_combined.utm_campaign_date,
    person_touchpoint_combined.actual_cost,
    person_touchpoint_combined.alliance_partner_name,
    person_touchpoint_combined.bizible_ad_campaign_name,
    person_touchpoint_combined.bizible_form_url,
    person_touchpoint_combined.bizible_form_url_raw,
    person_touchpoint_combined.bizible_integrated_campaign_grouping,
    person_touchpoint_combined.bizible_landing_page,
    person_touchpoint_combined.bizible_landing_page_raw,
    person_touchpoint_combined.bizible_marketing_channel,
    person_touchpoint_combined.bizible_marketing_channel_path,
    person_touchpoint_combined.bizible_referrer_page,
    person_touchpoint_combined.bizible_referrer_page_raw,
    person_touchpoint_combined.bizible_salesforce_campaign,
    person_touchpoint_combined.bizible_touchpoint_position,
    person_touchpoint_combined.bizible_touchpoint_type,
    person_touchpoint_combined.budgeted_cost,
    person_touchpoint_combined.integrated_campaign_region,
    person_touchpoint_combined.campaign_sub_region,
    person_touchpoint_combined.campaign_owner,
    person_touchpoint_combined.campaign_owner_manager,
    person_touchpoint_combined.channel_partner_name,
    person_touchpoint_combined.crm_person_status,
    person_touchpoint_combined.mql_sum,
    person_touchpoint_combined.devrel_campaign_type,
    person_touchpoint_combined.integrated_gtm,
    person_touchpoint_combined.integrated_budget_holder,
    person_touchpoint_combined.is_a_channel_partner_involved,
    person_touchpoint_combined.is_an_alliance_partner_involved,
    person_touchpoint_combined.keystone_type,
    person_touchpoint_combined.keystone_url_slug,
    person_touchpoint_combined.marketing_review_channel_grouping,
    person_touchpoint_combined.touchpoint_offer_type,
    person_touchpoint_combined.touchpoint_offer_type_grouped,
    person_touchpoint_combined.touchpoint_segment,
    person_touchpoint_combined.utm_allptnr,
    person_touchpoint_combined.utm_campaign,
    person_touchpoint_combined.utm_campaign_agency,
    person_touchpoint_combined.utm_campaign_language,
    person_touchpoint_combined.utm_campaign_name,
    person_touchpoint_combined.utm_content,
    person_touchpoint_combined.utm_content_asset_type,
    person_touchpoint_combined.utm_content_industry,
    person_touchpoint_combined.utm_content_offer,
    person_touchpoint_combined.utm_medium,
    person_touchpoint_combined.utm_partnerid,
    person_touchpoint_combined.utm_source,
    person_touchpoint_combined.bizible_count_custom_model,
    person_touchpoint_combined.bizible_weight_first_touch,
    person_touchpoint_combined.data_driven_model_weight,
    person_touchpoint_combined.gitlab_model_weight,
    person_touchpoint_combined.is_mgp_channel_based,
    person_touchpoint_combined.is_mgp_opportunity,
    person_touchpoint_combined.time_decay_model_weight,
    person_touchpoint_combined.touchpoint_sales_stage,
    person_touchpoint_combined.persona_category,
    person_touchpoint_combined.persona_is_management, 


    -- INQ date
    person_touchpoint_combined.inq_fiscal_quarter_name,
    person_touchpoint_combined.inq_day_of_fiscal_quarter_normalised,
    person_touchpoint_combined.inq_day_of_fiscal_year_normalised,
    person_touchpoint_combined.inq_previous_fiscal_quarter_name,
    person_touchpoint_combined.inq_previous_fiscal_year_quarter_name,
    -- MQL date
    person_touchpoint_combined.mql_fiscal_quarter_name,
    person_touchpoint_combined.mql_day_of_fiscal_quarter_normalised,
    person_touchpoint_combined.mql_day_of_fiscal_year_normalised,
    person_touchpoint_combined.mql_previous_fiscal_quarter_name,
    person_touchpoint_combined.mql_previous_fiscal_year_quarter_name,
    -- Touchpoint Date
    person_touchpoint_combined.touchpoint_fiscal_quarter_name,
    person_touchpoint_combined.touchpoint_day_of_fiscal_quarter_normalised,
    person_touchpoint_combined.touchpoint_day_of_fiscal_year_normalised,
    person_touchpoint_combined.touchpoint_previous_fiscal_quarter_name,
    person_touchpoint_combined.touchpoint_previous_fiscal_year_quarter_name,


    -- Worked Date and State
    rpt_sales_dev_activity_worked.worked_mql_person_id,
    rpt_sales_dev_activity_worked.last_activity_date,
  
    opportunity_base.dim_crm_opportunity_id,
    opportunity_base.order_type,
    opportunity_base.pipeline_created_date,
    opportunity_base.sales_accepted_date,
    opportunity_base.sales_qualified_source_name,
    opportunity_base.sdr_sqs_or_not,
    opportunity_base.sdr_or_bdr,
    opportunity_base.stage_name,
    opportunity_base.close_date,
    opportunity_base.close_fiscal_quarter_name,
    opportunity_base.created_date AS opportunity_created_date,
    opportunity_base.subscription_type,
    opportunity_base.forecast_category_name,
    opportunity_base.days_in_stage,
    opportunity_base.fpa_master_bookings_flag,
    opportunity_base.is_booked_net_arr,
    opportunity_base.is_closed,
    opportunity_base.is_credit,
    opportunity_base.is_edu_oss,
    opportunity_base.is_eligible_age_analysis,
    opportunity_base.is_net_arr_pipeline_created,
    opportunity_base.is_eligible_open_pipeline,
    opportunity_base.is_lost,
    opportunity_base.is_open,
    opportunity_base.new_logo_count,
    opportunity_base.new_logo_count_snapshot,
    opportunity_base.is_refund,
    opportunity_base.is_registration_from_portal,
    opportunity_base.is_renewal,
    opportunity_base.is_sao,
    opportunity_base.is_web_portal_purchase,
    opportunity_base.is_won,
    opportunity_base.opportunity_category,
    opportunity_base.opportunity_name,
    opportunity_base.parent_crm_account_geo_pubsec_segment,
    opportunity_base.pipe_council_grouping,

    opportunity_base.pipeline_created_fiscal_quarter_name,
    opportunity_base.pipeline_created_fiscal_year,    
    previous_quarter_date.previous_fiscal_quarter_name_fy      AS pipeline_previous_fiscal_quarter_name,
    previous_quarter_date.previous_fiscal_year_quarter_name_fy AS pipeline_previous_fiscal_year_quarter_name,

    opportunity_base.day_of_fiscal_quarter_normalised AS pipeline_day_of_fiscal_quarter_normalised,
    opportunity_base.day_of_fiscal_year_normalised    AS pipeline_day_of_fiscal_year_normalised,
    single_row_date_helper.current_day_of_fiscal_quarter_normalised,
    opportunity_base.report_area, -- to be used for opportunity / sales reporting 
    opportunity_base.report_geo,
    opportunity_base.report_region,
    opportunity_base.report_role_level_1,
    opportunity_base.report_role_level_2,
    opportunity_base.report_role_level_3,
    opportunity_base.report_segment,
    opportunity_base.pipeline_net_arr,
    opportunity_base.pipeline_net_arr_qtd,
    opportunity_base.net_arr,
    -- Marketing attribution metrics    
    IFNULL(person_touchpoint_combined.gitlab_model_weight, 0) * IFNULL(opportunity_base.pipeline_net_arr, 0) AS marketing_generated_pipeline,
    IFNULL(person_touchpoint_combined.gitlab_model_weight, 0) * IFNULL(opportunity_base.pipeline_net_arr_qtd, 0) AS marketing_generated_pipeline_qtd
  FROM person_touchpoint_combined
  FULL OUTER JOIN opportunity_base
    ON person_touchpoint_combined.dim_crm_opportunity_id = opportunity_base.dim_crm_opportunity_id
  LEFT JOIN mart_crm_account
    ON IFNULL(opportunity_base.dim_crm_account_id, person_touchpoint_combined.dim_crm_account_id) = mart_crm_account.dim_crm_account_id
  LEFT JOIN rpt_sales_dev_activity_worked
    ON person_touchpoint_combined.dim_crm_person_id = rpt_sales_dev_activity_worked.worked_mql_person_id
  LEFT JOIN previous_quarter_date
    ON opportunity_base.pipeline_created_fiscal_quarter_name = previous_quarter_date.fiscal_quarter_name_fy
  CROSS JOIN single_row_date_helper
)

SELECT *
FROM final