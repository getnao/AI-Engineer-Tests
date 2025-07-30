{{ config(materialized='table') }}

{{ simple_cte([
    ('rpt_crm_opportunity_pipeline_snapshot','rpt_crm_opportunity_pipeline_snapshot'),
    ('dim_crm_account_daily_snapshot','dim_crm_account_daily_snapshot'),
    ('dim_crm_user','dim_crm_user'),
    ('mart_crm_person','mart_crm_person'),
    ('sfdc_lead','sfdc_lead'),
    ('mart_crm_account','mart_crm_account'),
    ('mart_crm_event','mart_crm_event'),
    ('mart_crm_task','mart_crm_task'),
    ('mart_team_member_directory','mart_team_member_directory'),
    ('bdg_crm_opportunity_contact_role','bdg_crm_opportunity_contact_role'),
    ('dim_date', 'dim_date'),
    ('dim_sales_dev_user_hierarchy', 'dim_sales_dev_user_hierarchy'),
    ('wk_marketo_activity_change_score', 'wk_marketo_activity_change_score'),
    ('map_person_territory', 'map_person_territory')
  ]) 
}},

snapshot_dates AS (
--Snapshot on the 4th day of the current quarter for final previous quarter's numbers.
  SELECT
    date_day,
    LAG(fiscal_year, 1) OVER (ORDER BY date_day)            AS fiscal_year,
    LAG(fiscal_quarter, 1) OVER (ORDER BY date_day)         AS fiscal_quarter,
    LAG(fiscal_quarter_name_fy, 1) OVER (ORDER BY date_day) AS fiscal_quarter_name_fy
  FROM dim_date
  WHERE (
    is_third_business_day_of_fiscal_quarter = 1
    AND date_day <= current_date_actual
  )
  AND date_day BETWEEN '2023-01-31' AND CURRENT_DATE - 1
  QUALIFY LAG(fiscal_quarter_name_fy, 1) OVER (ORDER BY date_day) IS NOT NULL

),

account_snapshot_base AS (

  SELECT
    dim_crm_account_daily_snapshot.dim_crm_account_id,
    IFF(dim_crm_account_daily_snapshot.bdr_prospecting_status = 'Actively Working', TRUE, FALSE) AS is_actively_working_bdr_status,
    dim_crm_account_daily_snapshot.six_sense_account_buying_stage,
    dim_crm_account_daily_snapshot.snapshot_date
  FROM dim_crm_account_daily_snapshot
  INNER JOIN snapshot_dates
    ON dim_crm_account_daily_snapshot.snapshot_date = snapshot_dates.date_day

),

sales_dev_opps AS (

  SELECT
    rpt_crm_opportunity_pipeline_snapshot.dim_crm_account_id,
    rpt_crm_opportunity_pipeline_snapshot.bdr_prospecting_status,
    rpt_crm_opportunity_pipeline_snapshot.dim_crm_opportunity_id,
    rpt_crm_opportunity_pipeline_snapshot.sales_accepted_date,
    rpt_crm_opportunity_pipeline_snapshot.sales_accepted_fiscal_quarter_name,
    rpt_crm_opportunity_pipeline_snapshot.sao_day_of_fiscal_quarter,
    rpt_crm_opportunity_pipeline_snapshot.sao_day_of_fiscal_year,
    rpt_crm_opportunity_pipeline_snapshot.sao_fiscal_quarters_ago,
    rpt_crm_opportunity_pipeline_snapshot.pipeline_day_of_fiscal_quarter,
    rpt_crm_opportunity_pipeline_snapshot.pipeline_day_of_fiscal_year,
    rpt_crm_opportunity_pipeline_snapshot.pipeline_fiscal_quarters_ago,
    rpt_crm_opportunity_pipeline_snapshot.stage_0_pending_acceptance_date,
    rpt_crm_opportunity_pipeline_snapshot.stage_0_pending_acceptance_month,
    rpt_crm_opportunity_pipeline_snapshot.stage_0_pending_acceptance_fiscal_quarter_name,
    rpt_crm_opportunity_pipeline_snapshot.stage_1_discovery_date,
    rpt_crm_opportunity_pipeline_snapshot.stage_1_discovery_month,
    rpt_crm_opportunity_pipeline_snapshot.stage_1_discovery_fiscal_quarter_name,
    rpt_crm_opportunity_pipeline_snapshot.stage_2_scoping_date,
    rpt_crm_opportunity_pipeline_snapshot.stage_2_scoping_month,
    rpt_crm_opportunity_pipeline_snapshot.stage_2_scoping_fiscal_quarter_name,
    rpt_crm_opportunity_pipeline_snapshot.stage_3_technical_evaluation_date,
    rpt_crm_opportunity_pipeline_snapshot.stage_3_technical_evaluation_month,
    rpt_crm_opportunity_pipeline_snapshot.stage_3_technical_evaluation_fiscal_quarter_name,
    rpt_crm_opportunity_pipeline_snapshot.pipeline_created_date,
    rpt_crm_opportunity_pipeline_snapshot.pipeline_created_month,
    rpt_crm_opportunity_pipeline_snapshot.pipeline_created_fiscal_quarter_name,
    rpt_crm_opportunity_pipeline_snapshot.pipeline_created_fiscal_year,
    rpt_crm_opportunity_pipeline_snapshot.days_in_1_discovery,
    rpt_crm_opportunity_pipeline_snapshot.days_in_sao,
    rpt_crm_opportunity_pipeline_snapshot.days_since_last_activity,
    rpt_crm_opportunity_pipeline_snapshot.sales_qualified_source_name,
    rpt_crm_opportunity_pipeline_snapshot.sdr_sqs_or_not,
    rpt_crm_opportunity_pipeline_snapshot.report_segment,
    rpt_crm_opportunity_pipeline_snapshot.order_type,
    rpt_crm_opportunity_pipeline_snapshot.order_type_target_match    AS order_type_grouped,
    rpt_crm_opportunity_pipeline_snapshot.report_geo,
    rpt_crm_opportunity_pipeline_snapshot.report_region,
    rpt_crm_opportunity_pipeline_snapshot.report_area,
    rpt_crm_opportunity_pipeline_snapshot.report_geo_pubsec_segment,
    rpt_crm_opportunity_pipeline_snapshot.parent_crm_account_geo_pubsec_segment,
    rpt_crm_opportunity_pipeline_snapshot.report_role_level_1,
    rpt_crm_opportunity_pipeline_snapshot.report_role_level_2,
    rpt_crm_opportunity_pipeline_snapshot.report_role_level_3,
    rpt_crm_opportunity_pipeline_snapshot.pipe_council_grouping,
    rpt_crm_opportunity_pipeline_snapshot.parent_crm_account_territory,
    rpt_crm_opportunity_pipeline_snapshot.parent_crm_account_sales_segment,
    rpt_crm_opportunity_pipeline_snapshot.parent_crm_account_geo,
    rpt_crm_opportunity_pipeline_snapshot.parent_crm_account_region,
    rpt_crm_opportunity_pipeline_snapshot.parent_crm_account_area,
    rpt_crm_opportunity_pipeline_snapshot.deal_path_name,
    rpt_crm_opportunity_pipeline_snapshot.created_date                AS opp_created_date,
    rpt_crm_opportunity_pipeline_snapshot.close_date,
    rpt_crm_opportunity_pipeline_snapshot.close_day_of_fiscal_quarter,
    rpt_crm_opportunity_pipeline_snapshot.close_day_of_fiscal_year,
    rpt_crm_opportunity_pipeline_snapshot.close_fiscal_quarter_name,
    rpt_crm_opportunity_pipeline_snapshot.close_fiscal_quarters_ago,
    rpt_crm_opportunity_pipeline_snapshot.current_date_actual,
    rpt_crm_opportunity_pipeline_snapshot.current_day_of_fiscal_quarter,
    rpt_crm_opportunity_pipeline_snapshot.current_day_of_fiscal_year,
    rpt_crm_opportunity_pipeline_snapshot.new_logo_count,
    rpt_crm_opportunity_pipeline_snapshot.new_logo_count_snapshot,
    rpt_crm_opportunity_pipeline_snapshot.opportunity_category,
    rpt_crm_opportunity_pipeline_snapshot.stage_name,
    rpt_crm_opportunity_pipeline_snapshot.product_category,
    rpt_crm_opportunity_pipeline_snapshot.product_details,
    rpt_crm_opportunity_pipeline_snapshot.products_purchased,
    rpt_crm_opportunity_pipeline_snapshot.crm_account_focus_account,
    rpt_crm_opportunity_pipeline_snapshot.is_sao,
    rpt_crm_opportunity_pipeline_snapshot.is_booked_net_arr,
    rpt_crm_opportunity_pipeline_snapshot.is_net_arr_closed_deal,
    rpt_crm_opportunity_pipeline_snapshot.is_net_arr_pipeline_created,
    rpt_crm_opportunity_pipeline_snapshot.is_eligible_age_analysis,
    rpt_crm_opportunity_pipeline_snapshot.is_eligible_open_pipeline,
    rpt_crm_opportunity_pipeline_snapshot.crm_business_dev_rep_id     AS opportunity_business_development_representative,
    rpt_crm_opportunity_pipeline_snapshot.crm_sales_dev_rep_id        AS opportunity_sales_development_representative,
    rpt_crm_opportunity_pipeline_snapshot.bdr_next_steps,
    rpt_crm_opportunity_pipeline_snapshot.bdr_account_research,
    rpt_crm_opportunity_pipeline_snapshot.bdr_account_strategy,
    rpt_crm_opportunity_pipeline_snapshot.account_bdr_assigned_user_role,
    rpt_crm_opportunity_pipeline_snapshot.bdr_recycle_date,
    rpt_crm_opportunity_pipeline_snapshot.actively_working_start_date,
    rpt_crm_opportunity_pipeline_snapshot.is_sdr_first_order_booked_deal,
    rpt_crm_opportunity_pipeline_snapshot.sales_dev_bdr_or_sdr,
    rpt_crm_opportunity_pipeline_snapshot.sdr_bdr_user_id,
    rpt_crm_opportunity_pipeline_snapshot.is_sales_dev_qualified_opportunity,
    rpt_crm_opportunity_pipeline_snapshot.is_sales_dev_pipeline_created,
    NULL                                                               AS is_sales_dev_pipeline_created_qtd, -- Nullified as per requirement
    rpt_crm_opportunity_pipeline_snapshot.sales_accepted_opportunity_id,
    CASE 
      WHEN rpt_crm_opportunity_pipeline_snapshot.is_sales_dev_pipeline_created = TRUE 
      THEN rpt_crm_opportunity_pipeline_snapshot.dim_crm_opportunity_id
    END                                                                AS pipeline_opportunity_id,
    rpt_crm_opportunity_pipeline_snapshot.sdr_sao_id,
    rpt_crm_opportunity_pipeline_snapshot.bdr_first_order_sao_id,
    rpt_crm_opportunity_pipeline_snapshot.sdr_fo_booked_opportunity_id AS first_order_booked_opportunity_id,
    rpt_crm_opportunity_pipeline_snapshot.bdr_stage_1_net_arr,
    rpt_crm_opportunity_pipeline_snapshot.bdr_stage_3_net_arr,
    rpt_crm_opportunity_pipeline_snapshot.pipeline_net_arr,
    NULL                                                               AS pipeline_net_arr_qtd, -- Nullified as per requirement
    rpt_crm_opportunity_pipeline_snapshot.first_order_booked_net_arr,
    rpt_crm_opportunity_pipeline_snapshot.net_arr_live                AS net_arr,
    rpt_crm_opportunity_pipeline_snapshot.net_arr_stage_1,
    rpt_crm_opportunity_pipeline_snapshot.xdr_net_arr_stage_1,
    rpt_crm_opportunity_pipeline_snapshot.xdr_net_arr_stage_3
  FROM rpt_crm_opportunity_pipeline_snapshot
  WHERE (rpt_crm_opportunity_pipeline_snapshot.sdr_bdr_user_id IS NOT NULL 
         OR rpt_crm_opportunity_pipeline_snapshot.sales_qualified_source_name = 'SDR Generated')
    AND (rpt_crm_opportunity_pipeline_snapshot.stage_1_discovery_date >= '2023-02-01' 
         OR rpt_crm_opportunity_pipeline_snapshot.pipeline_created_date >= '2023-02-01')

),

merged_person_base AS (

  SELECT
    mart_crm_person.dim_crm_person_id,
    sfdc_lead.converted_contact_id AS sfdc_record_id,
    sfdc_lead.lead_id              AS original_lead_id,
    sales_dev_opps.dim_crm_opportunity_id,
    sales_dev_opps.opp_created_date,
    mart_crm_person.dim_crm_account_id
  FROM sfdc_lead
  LEFT JOIN mart_crm_person
    ON sfdc_lead.converted_contact_id = mart_crm_person.sfdc_record_id
  LEFT JOIN sales_dev_opps
    ON converted_opportunity_id = dim_crm_opportunity_id
  WHERE converted_contact_id IS NOT NULL

),

contacts_on_opps AS (

  SELECT
    bdg_crm_opportunity_contact_role.sfdc_record_id,
    bdg_crm_opportunity_contact_role.dim_crm_person_id,
    bdg_crm_opportunity_contact_role.contact_role,
    bdg_crm_opportunity_contact_role.is_primary_contact,
    sales_dev_opps.dim_crm_opportunity_id,
    sales_dev_opps.opp_created_date,
    sales_dev_opps.sales_accepted_date,
    sales_dev_opps.sdr_bdr_user_id AS dim_crm_user_id
  FROM bdg_crm_opportunity_contact_role
  INNER JOIN sales_dev_opps
    ON bdg_crm_opportunity_contact_role.dim_crm_opportunity_id = sales_dev_opps.dim_crm_opportunity_id

),

activity_base AS (

  SELECT
    mart_crm_event.event_id                    AS activity_id,
    mart_crm_event.dim_crm_user_id,
    mart_crm_event.dim_crm_opportunity_id,
    mart_crm_event.dim_crm_account_id,
    mart_crm_event.sfdc_record_id,
    mart_crm_event.dim_crm_person_id,
    dim_crm_user.dim_crm_user_id               AS booked_by_user_id,
    mart_crm_event.event_date                  AS activity_date,
    dim_date.day_of_fiscal_quarter_normalised  AS activity_day_of_fiscal_quarter,
    dim_date.fiscal_quarter_name_fy            AS activity_fiscal_quarter_name,
    'Event'                                    AS activity_type,
    mart_crm_event.event_type                  AS activity_subtype
  FROM mart_crm_event
  LEFT JOIN dim_crm_user
    ON booked_by_employee_number = dim_crm_user.employee_number
  LEFT JOIN dim_date
    ON mart_crm_event.event_date = dim_date.date_day
  INNER JOIN sales_dev_opps
    ON mart_crm_event.dim_crm_user_id = sales_dev_opps.sdr_bdr_user_id
      OR booked_by_user_id = sales_dev_opps.sdr_bdr_user_id
  WHERE activity_date BETWEEN '2023-01-01' AND CURRENT_DATE
  UNION
  SELECT
    mart_crm_task.task_id                      AS activity_id,
    mart_crm_task.dim_crm_user_id,
    mart_crm_task.dim_crm_opportunity_id,
    mart_crm_task.dim_crm_account_id,
    mart_crm_task.sfdc_record_id,
    mart_crm_task.dim_crm_person_id,
    NULL                                       AS booked_by_user_id,
    DATE(mart_crm_task.task_completed_date)    AS activity_date,
    dim_date.day_of_fiscal_quarter_normalised  AS activity_day_of_fiscal_quarter,
    dim_date.fiscal_quarter_name_fy            AS activity_fiscal_quarter_name,
    mart_crm_task.task_type                    AS activity_type,
    mart_crm_task.task_subtype                 AS activity_subtype
  FROM mart_crm_task
  INNER JOIN sales_dev_opps
    ON mart_crm_task.dim_crm_user_id = sales_dev_opps.sdr_bdr_user_id
  LEFT JOIN dim_date
    ON DATE(mart_crm_task.task_completed_date) = dim_date.date_day
  WHERE activity_date BETWEEN '2023-01-01' AND CURRENT_DATE
--we are restricting the entire datasource to the last 2 years of tasks and opportunities for size purposes, as usually last 2 fiscal years is all that is relevant for analysis.

),

activity_final AS (

  SELECT
    activity_base.activity_id,
    COALESCE(activity_base.booked_by_user_id, activity_base.dim_crm_user_id)                              AS dim_crm_user_id,
    mart_crm_person.dim_crm_person_id,
    COALESCE(mart_crm_person.sfdc_record_id, activity_base.sfdc_record_id)                                AS sfdc_record_id,
    COALESCE(mart_crm_person.dim_crm_account_id, activity_base.dim_crm_account_id)                        AS dim_crm_account_id,
    IFF(activity_base.activity_date >= mart_crm_person.mql_date_first_pt, TRUE, FALSE)                    AS worked_after_mql_flag,
    IFF(activity_base.activity_date >= mart_crm_person.inquiry_date_pt, TRUE, FALSE)                      AS worked_after_inquiry_flag,
    activity_base.activity_date::DATE                                                                     AS activity_date,
    activity_base.activity_day_of_fiscal_quarter,
    activity_base.activity_fiscal_quarter_name,
    activity_base.activity_type,
    activity_base.activity_subtype
  FROM activity_base
  LEFT JOIN merged_person_base
    ON activity_base.sfdc_record_id = merged_person_base.original_lead_id
  LEFT JOIN mart_crm_person
    ON COALESCE(merged_person_base.dim_crm_person_id, activity_base.dim_crm_person_id) = mart_crm_person.dim_crm_person_id

),

opp_to_lead AS (

  SELECT
    sales_dev_opps.*,
    merged_person_base.dim_crm_person_id                                                                                 AS converted_person_id,
    contacts_on_opps.dim_crm_person_id                                                                                   AS contact_person_id,
    activity_final.dim_crm_person_id                                                                                     AS activity_person_id,
    COALESCE(merged_person_base.dim_crm_person_id, contacts_on_opps.dim_crm_person_id, activity_final.dim_crm_person_id) AS waterfall_person_id,
    COALESCE(DATEDIFF(DAY, activity_date, sales_dev_opps.stage_1_discovery_date), 0)                                     AS activity_to_sao_days,
    account_snapshot_base.is_actively_working_bdr_status,
    account_snapshot_base.six_sense_account_buying_stage
  FROM sales_dev_opps
  LEFT JOIN merged_person_base
    ON sales_dev_opps.dim_crm_opportunity_id = merged_person_base.dim_crm_opportunity_id
  LEFT JOIN contacts_on_opps
    ON sales_dev_opps.dim_crm_opportunity_id = contacts_on_opps.dim_crm_opportunity_id
  LEFT JOIN activity_final
    ON sales_dev_opps.dim_crm_account_id = activity_final.dim_crm_account_id
      AND sales_dev_opps.stage_1_discovery_date >= activity_final.activity_date
      AND sales_dev_opps.sdr_bdr_user_id = activity_final.dim_crm_user_id
  LEFT JOIN account_snapshot_base
    ON activity_final.dim_crm_account_id = account_snapshot_base.dim_crm_account_id
      AND activity_final.activity_date = account_snapshot_base.snapshot_date

),

opps_missing_link AS (

  SELECT *
  FROM opp_to_lead
  WHERE waterfall_person_id IS NULL OR activity_to_sao_days > 90 --adds back in the opps that are being discarded due to a too long delay from activity on the lead for that lead to be credited with SAO creation

),

sixsense_6qa_lead_flag_base AS (

  SELECT
    mart_crm_person.dim_crm_person_id,
    mart_crm_person.sfdc_record_id,
    wk_marketo_activity_change_score.new_value - wk_marketo_activity_change_score.old_value AS score_change,
    MIN(activity_date)                                                                      AS first_6qa_score_date,
    MAX(activity_date)                                                                      AS last_6qa_score_date
  FROM
    wk_marketo_activity_change_score
  LEFT JOIN mart_crm_person
    ON wk_marketo_activity_change_score.lead_id = mart_crm_person.marketo_lead_id
  WHERE reason = 'Changed by Smart Campaign OP-Scoring_2020.6QA Identified action Change Score' AND primary_attribute_value = 'Behavior Score'
  GROUP BY ALL



),

final AS (

  SELECT DISTINCT
    mart_crm_person.dim_crm_person_id,
    mart_crm_person.sfdc_record_id,
    COALESCE(opp_to_lead.dim_crm_account_id, mart_crm_person.dim_crm_account_id)                           AS dim_crm_account_id,
    mart_crm_account.bdr_prospecting_status,
    mart_crm_person.mql_date_latest,
    DATEADD(day, 45, mart_crm_person.mql_date_first_pt)                                                    AS lto_date,
    dim_lto_date.fiscal_quarter_name_fy                                                                    AS lto_fiscal_quarter_name,
    dim_lto_date.day_of_fiscal_quarter_normalised                                                          AS lto_day_of_fiscal_quarter,
    CASE 
        WHEN 
        opp_to_lead.stage_1_discovery_date<=DATEADD(day, 45, mart_crm_person.mql_date_first_pt) 
        AND opp_to_lead.stage_1_discovery_date>=mart_crm_person.mql_date_first_pt
        THEN
        opp_to_lead.sales_accepted_opportunity_id
    END AS lto_saos,
        
    mql_date_first.day_of_fiscal_quarter_normalised                                                        AS mql_day_of_fiscal_quarter,
    mql_date_first.fiscal_quarter_name_fy                                                                  AS mql_fiscal_quarter_name,
    mql_date_first.fiscal_quarter_name_fy                                                                  AS first_mql_fiscal_quarter_name,
    mql_date_first.day_of_fiscal_quarter                                                                   AS first_mql_day_of_fiscal_quarter,
    mql_date_first.date_day                                                                                AS first_mql_date,
    mql_date_first.fiscal_quarters_ago                                                                     AS fiscal_quarters_ago_mql_date_first,
    mart_crm_person.inquiry_date_pt,
    mart_crm_person.high_priority_datetime,
    dim_inquiry_date.day_of_fiscal_quarter_normalised                                                      AS inquiry_day_of_fiscal_quarter,
    dim_inquiry_date.fiscal_quarter_name_fy                                                                AS inquiry_fiscal_quarter_name,
    mart_crm_person.account_demographics_sales_segment                                                     AS person_sales_segment,
    mart_crm_person.account_demographics_sales_segment_grouped                                             AS person_sales_segment_grouped,
    mart_crm_person.account_demographics_geo                                                               AS person_first_geo,
    map_person_territory.report_geo                                                                        AS report_person_geo, 
    map_person_territory.report_region                                                                     AS report_person_region,
    map_person_territory.report_area                                                                       AS report_person_area,
    map_person_territory.report_sales_segment                                                              AS report_person_sales_segment,
    mart_crm_person.is_mql,
    mart_crm_person.is_first_order_person,
    mart_crm_person.person_first_country,
    mart_crm_person.lead_score_classification,
    mart_crm_person.is_defaulted_trial,
    mart_crm_person.persona_category,
    mart_crm_person.is_management                                                                          AS persona_is_management,
    mart_crm_person.lead_source,
    mart_crm_person.status                                                                                 AS lead_status,
    mart_crm_person.bizible_mql_form_url,
    mart_crm_person.bizible_mql_ad_campaign_name,
    mart_crm_person.bizible_mql_marketing_channel,
    mart_crm_person.bizible_mql_marketing_channel_path,
    mart_crm_person.bizible_most_recent_form_url,
    mart_crm_person.bizible_most_recent_ad_campaign_name,
    mart_crm_person.bizible_most_recent_marketing_channel,
    mart_crm_person.bizible_most_recent_marketing_channel_path,
    mart_crm_person.source_buckets,
    mart_crm_person.email_domain_type,
    mart_crm_person.sfdc_record_type,
    mart_crm_person.mql_worked_by_user_id,
    mart_crm_person.mql_worked_by_user_manager_id,
    mart_crm_person.last_worked_by_date,
    mart_crm_person.last_worked_by_datetime,
    mart_crm_person.last_worked_by_user_manager_id,
    mart_crm_person.last_worked_by_user_id,
    COALESCE (mart_crm_person.propensity_to_purchase_score_group, 'No PTP Score')                          AS propensity_to_purchase_score_group,
    COALESCE (propensity_to_purchase_score_group = '4' OR propensity_to_purchase_score_group = '5', FALSE) AS is_high_ptp_lead,
    mart_crm_person.marketo_last_interesting_moment,
    mart_crm_person.marketo_last_interesting_moment_date,
    COALESCE (sixsense_6qa_lead_flag_base.dim_crm_person_id IS NOT NULL, FALSE)                            AS is_6qa_scored_lead,
    sixsense_6qa_lead_flag_base.first_6qa_score_date,
    sixsense_6qa_lead_flag_base.last_6qa_score_date,
    activity_final.dim_crm_user_id,
    activity_final.activity_date,
    activity_final.activity_type,
    activity_final.activity_subtype,
    activity_final.activity_id,
    activity_final.activity_day_of_fiscal_quarter,
    activity_final.activity_fiscal_quarter_name,
    NULL                                                                                                   AS tasks_completed,
    IFNULL(activity_final.worked_after_mql_flag, FALSE)                                                    AS worked_after_mql_flag,
    IFNULL(activity_final.worked_after_inquiry_flag, FALSE)                                                AS worked_after_inquiry_flag,
    IFF(activity_final.worked_after_mql_flag = TRUE, mart_crm_person.dim_crm_person_id, NULL)              AS worked_mql_person_id,
    IFF(activity_final.worked_after_inquiry_flag = TRUE, mart_crm_person.dim_crm_person_id, NULL)          AS worked_inquiry_person_id,
    mart_crm_account.parent_crm_account_territory,
    mart_crm_account.parent_crm_account_sales_segment,
    mart_crm_account.parent_crm_account_geo,
    mart_crm_account.parent_crm_account_region,
    mart_crm_account.parent_crm_account_area,
    mart_crm_account.abm_tier,
    mart_crm_account.crm_account_owner_id,
    mart_crm_account.crm_account_owner,
    mart_crm_account.owner_role,
    mart_crm_account.crm_account_name,
    mart_crm_account.crm_account_focus_account,
    mart_crm_account.crm_account_owner_user_segment,
    mart_crm_account.six_sense_account_profile_fit,
    mart_crm_account.six_sense_account_reach_score,
    mart_crm_account.six_sense_account_profile_score,
    mart_crm_account.six_sense_account_buying_stage,
    mart_crm_account.six_sense_account_numerical_reach_score,
    mart_crm_account.six_sense_account_update_date,
    mart_crm_account.six_sense_account_6_qa_start_date,
    mart_crm_account.six_sense_account_6_qa_end_date,
    mart_crm_account.six_sense_account_6_qa_age_days,
    mart_crm_account.six_sense_account_intent_score,
    mart_crm_account.six_sense_segments,
    mart_crm_account.pte_score_group,
    mart_crm_account.is_sdr_target_account,
    mart_crm_account.is_first_order_available,
    mart_crm_account.is_base_prospect_account,
    mart_crm_account.crm_account_type,
    mart_crm_account.crm_account_industry,
    mart_crm_account.crm_account_sub_industry,
    mart_crm_account.bdr_next_steps,
    mart_crm_account.bdr_account_research,
    mart_crm_account.bdr_account_strategy,
    mart_crm_account.account_bdr_assigned_user_role,
    mart_crm_account.bdr_recycle_date,
    mart_crm_account.actively_working_start_date,
    opp_to_lead.dim_crm_opportunity_id,
    opp_to_lead.sdr_sao_id,
    opp_to_lead.bdr_first_order_sao_id,
    opp_to_lead.sales_accepted_opportunity_id,
    opp_to_lead.pipeline_opportunity_id,
    opp_to_lead.first_order_booked_opportunity_id,
    CASE
      WHEN 
        activity_final.worked_after_mql_flag = TRUE AND 
        DATEDIFF(DAY,mart_crm_person.mql_date_first_pt,opp_to_lead.stage_1_discovery_date) <= 45 AND 
        opp_to_lead.stage_1_discovery_date >= mart_crm_person.mql_date_first_pt
      THEN sales_accepted_opportunity_id
    END AS mql_sales_accepted_opportunity_id, 
    opp_to_lead.sdr_bdr_user_id,
    opp_to_lead.net_arr,
    opp_to_lead.first_order_booked_net_arr,
    opp_to_lead.pipeline_net_arr,
    opp_to_lead.pipeline_net_arr_qtd,
    opp_to_lead.bdr_stage_1_net_arr,
    opp_to_lead.bdr_stage_3_net_arr,
    opp_to_lead.net_arr_stage_1,
    opp_to_lead.xdr_net_arr_stage_1,
    opp_to_lead.xdr_net_arr_stage_3,
    opp_to_lead.sales_accepted_date,
    opp_to_lead.sales_accepted_fiscal_quarter_name,
    opp_to_lead.sao_day_of_fiscal_quarter,
    opp_to_lead.sao_day_of_fiscal_year,
    opp_to_lead.sao_fiscal_quarters_ago,
    opp_to_lead.pipeline_day_of_fiscal_quarter,
    opp_to_lead.pipeline_day_of_fiscal_year,
    opp_to_lead.pipeline_fiscal_quarters_ago,
    --If an MQL has been created in a quarter before pipeline creation or in the same quarter, then it is inbound;  otherwise, it is outbound (generated by SDR).
    MIN(IFF(activity_final.worked_after_mql_flag = TRUE, mql_date_first.fiscal_quarters_ago, NULL)) 
        OVER (PARTITION BY opp_to_lead.dim_crm_opportunity_id) AS min_mql_fiscal_quarters_ago,
    CASE 
        WHEN (min_mql_fiscal_quarters_ago-opp_to_lead.pipeline_fiscal_quarters_ago BETWEEN 0 AND 1)
        THEN 'Inbound' 
        WHEN opp_to_lead.dim_crm_opportunity_id IS NOT NULL 
        THEN 'Outbound'
    END AS opportunity_source_type,
    opp_to_lead.stage_0_pending_acceptance_date,
    opp_to_lead.stage_0_pending_acceptance_month,
    opp_to_lead.stage_0_pending_acceptance_fiscal_quarter_name,
    opp_to_lead.stage_1_discovery_date,
    opp_to_lead.stage_1_discovery_month,
    opp_to_lead.stage_1_discovery_fiscal_quarter_name,
    opp_to_lead.stage_2_scoping_date,
    opp_to_lead.stage_2_scoping_month,
    opp_to_lead.stage_2_scoping_fiscal_quarter_name,
    opp_to_lead.stage_3_technical_evaluation_date,
    opp_to_lead.stage_3_technical_evaluation_month,
    opp_to_lead.stage_3_technical_evaluation_fiscal_quarter_name,
    opp_to_lead.pipeline_created_date,
    opp_to_lead.pipeline_created_month,
    opp_to_lead.pipeline_created_fiscal_quarter_name,
    opp_to_lead.pipeline_created_fiscal_year,
    opp_to_lead.days_in_1_discovery,
    opp_to_lead.days_in_sao,
    opp_to_lead.days_since_last_activity,
    opp_to_lead.sales_qualified_source_name,
    opp_to_lead.sdr_sqs_or_not,
    opp_to_lead.report_segment,
    opp_to_lead.report_geo,
    opp_to_lead.report_region,
    opp_to_lead.report_area,
    opp_to_lead.parent_crm_account_geo_pubsec_segment,
    opp_to_lead.report_role_level_1,
    opp_to_lead.report_role_level_2,
    opp_to_lead.report_role_level_3,
    opp_to_lead.pipe_council_grouping,
    opp_to_lead.deal_path_name,
    opp_to_lead.opp_created_date,
    opp_to_lead.close_date,
    opp_to_lead.close_day_of_fiscal_quarter,
    opp_to_lead.close_day_of_fiscal_year,
    opp_to_lead.close_fiscal_quarter_name,
    opp_to_lead.close_fiscal_quarters_ago,
    opp_to_lead.current_date_actual,
    opp_to_lead.current_day_of_fiscal_quarter,
    opp_to_lead.current_day_of_fiscal_year,
    opp_to_lead.activity_to_sao_days,
    opp_to_lead.order_type,
    opp_to_lead.order_type_grouped,
    opp_to_lead.new_logo_count,
    opp_to_lead.new_logo_count_snapshot,
    opp_to_lead.opportunity_category,
    opp_to_lead.stage_name,
    opp_to_lead.product_category,
    opp_to_lead.product_details,
    opp_to_lead.products_purchased,
    opp_to_lead.sales_dev_bdr_or_sdr,
    opp_to_lead.opportunity_sales_development_representative,
    opp_to_lead.opportunity_business_development_representative,
    opp_to_lead.is_sao,
    opp_to_lead.is_sales_dev_qualified_opportunity,
    opp_to_lead.is_sdr_first_order_booked_deal,
    opp_to_lead.is_booked_net_arr,
    opp_to_lead.is_net_arr_closed_deal,
    opp_to_lead.is_net_arr_pipeline_created,
    opp_to_lead.is_eligible_age_analysis,
    opp_to_lead.is_eligible_open_pipeline,
    opp_to_lead.is_actively_working_bdr_status AS activity_six_sense_account_buying_stage,
    opp_to_lead.six_sense_account_buying_stage AS activity_is_actively_worked_account,
    opportunity_snapshot_hierarchy.dim_crm_user_id                                                         AS sales_dev_rep_user_id,
    opportunity_snapshot_hierarchy.sales_dev_rep_role_name,
    opportunity_snapshot_hierarchy.sales_dev_rep_email,
    opportunity_snapshot_hierarchy.sales_dev_rep_user_full_name                                            AS sales_dev_rep_full_name,
    opportunity_snapshot_hierarchy.sales_dev_rep_title,
    opportunity_snapshot_hierarchy.sales_dev_rep_department,
    opportunity_snapshot_hierarchy.sales_dev_rep_team,
    opportunity_snapshot_hierarchy.sales_dev_rep_is_active,
    opportunity_snapshot_hierarchy.sales_dev_rep_user_role_level_1,
    opportunity_snapshot_hierarchy.sales_dev_rep_user_role_level_2,
    opportunity_snapshot_hierarchy.sales_dev_rep_user_role_level_3,
    opportunity_snapshot_hierarchy.crm_user_sales_segment,
    opportunity_snapshot_hierarchy.crm_user_geo,
    opportunity_snapshot_hierarchy.crm_user_region,
    opportunity_snapshot_hierarchy.crm_user_area,
    opportunity_snapshot_hierarchy.sales_dev_rep_employee_number,
    opportunity_snapshot_hierarchy.sales_dev_rep_direct_manager_id,
    opportunity_snapshot_hierarchy.sales_dev_rep_manager_full_name                                         AS sales_dev_manager_full_name,
    opportunity_snapshot_hierarchy.sales_dev_manager_email,
    opportunity_snapshot_hierarchy.sales_dev_manager_employee_number,
    opportunity_snapshot_hierarchy.sales_dev_manager_user_role_name,
    opportunity_snapshot_hierarchy.sales_dev_leader_id,
    opportunity_snapshot_hierarchy.sales_dev_leader_user_role_name,
    opportunity_snapshot_hierarchy.sales_dev_rep_leader_full_name                                          AS sales_dev_leader_full_name,
    opportunity_snapshot_hierarchy.sales_dev_leader_employee_number,
    opportunity_snapshot_hierarchy.sales_dev_leader_email,
    activity_snapshot_hierarchy.dim_crm_user_id                                                            AS activity_sales_dev_rep_user_id,
    activity_snapshot_hierarchy.sales_dev_rep_role_name                                                    AS activity_sales_dev_rep_role_name,
    activity_snapshot_hierarchy.sales_dev_rep_email                                                        AS activity_sales_dev_rep_email,
    activity_snapshot_hierarchy.sales_dev_rep_user_full_name                                               AS activity_sales_dev_rep_full_name,
    activity_snapshot_hierarchy.sales_dev_rep_manager_full_name                                            AS activity_sales_dev_manager_full_name,
    activity_snapshot_hierarchy.sales_dev_rep_leader_full_name                                             AS activity_sales_dev_leader_full_name,
    activity_snapshot_hierarchy.sales_dev_rep_user_role_level_1                                            AS activity_sales_dev_rep_user_role_level_1,
    activity_snapshot_hierarchy.sales_dev_rep_user_role_level_2                                            AS activity_sales_dev_rep_user_role_level_2,
    activity_snapshot_hierarchy.sales_dev_rep_user_role_level_3                                            AS activity_sales_dev_rep_user_role_level_3

  FROM mart_crm_person
  LEFT JOIN map_person_territory
    ON mart_crm_person.dim_crm_person_id = map_person_territory.dim_crm_person_id
  LEFT JOIN dim_date AS mql_date_first
    ON mart_crm_person.mql_date_first_pt = mql_date_first.date_day
  LEFT JOIN dim_date AS dim_inquiry_date
    ON mart_crm_person.inquiry_date_pt = dim_inquiry_date.date_day
  LEFT JOIN dim_date as dim_lto_date
    ON lto_date = dim_lto_date.date_day
  LEFT JOIN sixsense_6qa_lead_flag_base
    ON mart_crm_person.dim_crm_person_id = sixsense_6qa_lead_flag_base.dim_crm_person_id
  LEFT JOIN activity_final
    ON mart_crm_person.dim_crm_person_id = activity_final.dim_crm_person_id
  LEFT JOIN opp_to_lead
    ON mart_crm_person.dim_crm_person_id = opp_to_lead.waterfall_person_id
  LEFT JOIN mart_crm_account
    ON COALESCE(opp_to_lead.dim_crm_account_id, mart_crm_person.dim_crm_account_id) = mart_crm_account.dim_crm_account_id
  LEFT JOIN dim_sales_dev_user_hierarchy AS opportunity_snapshot_hierarchy
    ON opp_to_lead.sdr_bdr_user_id = opportunity_snapshot_hierarchy.dim_crm_user_id
      AND opp_to_lead.stage_1_discovery_date = opportunity_snapshot_hierarchy.snapshot_date

  LEFT JOIN dim_sales_dev_user_hierarchy AS activity_snapshot_hierarchy
    ON activity_final.dim_crm_user_id = activity_snapshot_hierarchy.dim_crm_user_id
      AND activity_final.activity_date = activity_snapshot_hierarchy.snapshot_date
  WHERE activity_to_sao_days <= 90 OR activity_to_sao_days IS NULL
  UNION
  SELECT DISTINCT -- distinct is necessary in order to not duplicate rows as addition of the rule above of activity_to_sao_days >90 might create multiple rows if there are multiple leads that satisfy the condition per opp which is not ideal. 
    NULL                                                           AS dim_crm_person_id,
    NULL                                                           AS sfdc_record_id,
    opps_missing_link.dim_crm_account_id,
    mart_crm_account.bdr_prospecting_status,
    NULL                                                           AS mql_date_latest,
    NULL                                                           AS lto_date,
    NULL                                                           AS lto_fiscal_quarter_name, 
    NULL                                                           AS lto_day_of_fiscal_quarter,
    NULL                                                           AS lto_saos,
    NULL                                                           AS mql_day_of_fiscal_quarter,
    NULL                                                           AS mql_fiscal_quarter_name,
    NULL                                                           AS first_mql_fiscal_quarter_name,
    NULL                                                           AS first_mql_day_of_fiscal_quarter,
    NULL                                                           AS first_mql_date,
    NULL                                                           AS fiscal_quarters_ago_mql_date_first,
    NULL                                                           AS inquiry_date_pt,
    NULL                                                           AS high_priority_datetime,
    NULL                                                           AS inquiry_day_of_fiscal_quarter,
    NULL                                                           AS inquiry_fiscal_quarter_name,
    NULL                                                           AS person_sales_segment,
    NULL                                                           AS person_sales_segment_grouped,
    NULL                                                           AS person_first_geo,
    NULL                                                           AS report_person_geo,
    NULL                                                           AS report_person_region,
    NULL                                                           AS report_person_area,
    NULL                                                           AS report_person_sales_segment,
    NULL                                                           AS is_mql,
    NULL                                                           AS is_first_order_person,
    NULL                                                           AS person_first_country,
    NULL                                                           AS lead_score_classification,
    NULL                                                           AS is_defaulted_trial,
    NULL                                                           AS persona_category,
    NULL                                                           AS persona_is_management,
    NULL                                                           AS lead_source,
    NULL                                                           AS lead_status,
    NULL                                                           AS bizible_mql_form_url,
    NULL                                                           AS bizible_mql_ad_campaign_name,
    NULL                                                           AS bizible_mql_marketing_channel,
    NULL                                                           AS bizible_mql_marketing_channel_path,
    NULL                                                           AS bizible_most_recent_form_url,
    NULL                                                           AS bizible_most_recent_ad_campaign_name,
    NULL                                                           AS bizible_most_recent_marketing_channel,
    NULL                                                           AS bizible_most_recent_marketing_channel_path,
    NULL                                                           AS source_buckets,
    NULL                                                           AS email_domain_type,
    NULL                                                           AS sfdc_record_type,
    NULL                                                           AS mql_worked_by_user_id,
    NULL                                                           AS mql_worked_by_user_manager_id,
    NULL                                                           AS last_worked_by_date,
    NULL                                                           AS last_worked_by_datetime,
    NULL                                                           AS last_worked_by_user_manager_id,
    NULL                                                           AS last_worked_by_user_id,
    NULL                                                           AS propensity_to_purchase_score_group,
    NULL                                                           AS is_high_ptp_lead,
    NULL                                                           AS marketo_last_interesting_moment,
    NULL                                                           AS marketo_last_interesting_moment_date,
    NULL                                                           AS is_6qa_scored_lead,
    NULL                                                           AS first_6qa_score_date,
    NULL                                                           AS last_6qa_score_date,
    NULL                                                           AS dim_crm_user_id,
    NULL                                                           AS activity_date,
    NULL                                                           AS activity_type,
    NULL                                                           AS activity_subtype,
    NULL                                                           AS activity_id,
    NULL                                                           AS activity_day_of_fiscal_quarter,
    NULL                                                           AS activity_fiscal_quarter_name,
    NULL                                                           AS tasks_completed,
    NULL                                                           AS worked_after_mql_flag,
    NULL                                                           AS worked_after_inquiry_flag,
    NULL                                                           AS worked_mql_person_id,
    NULL                                                           AS worked_inquiry_person_id,
    mart_crm_account.parent_crm_account_territory,
    mart_crm_account.parent_crm_account_sales_segment,
    mart_crm_account.parent_crm_account_geo,
    mart_crm_account.parent_crm_account_region,
    mart_crm_account.parent_crm_account_area,
    mart_crm_account.abm_tier,
    mart_crm_account.crm_account_owner_id,
    mart_crm_account.crm_account_owner,
    mart_crm_account.owner_role,
    mart_crm_account.crm_account_name,
    mart_crm_account.crm_account_focus_account,
    mart_crm_account.crm_account_owner_user_segment,
    mart_crm_account.six_sense_account_profile_fit,
    mart_crm_account.six_sense_account_reach_score,
    mart_crm_account.six_sense_account_profile_score,
    mart_crm_account.six_sense_account_buying_stage,
    mart_crm_account.six_sense_account_numerical_reach_score,
    mart_crm_account.six_sense_account_update_date,
    mart_crm_account.six_sense_account_6_qa_start_date,
    mart_crm_account.six_sense_account_6_qa_end_date,
    mart_crm_account.six_sense_account_6_qa_age_days,
    mart_crm_account.six_sense_account_intent_score,
    mart_crm_account.six_sense_segments,
    mart_crm_account.pte_score_group,
    mart_crm_account.is_sdr_target_account,
    mart_crm_account.is_first_order_available,
    mart_crm_account.is_base_prospect_account,
    mart_crm_account.crm_account_type,
    mart_crm_account.crm_account_industry,
    mart_crm_account.crm_account_sub_industry,
    mart_crm_account.bdr_next_steps,
    mart_crm_account.bdr_account_research,
    mart_crm_account.bdr_account_strategy,
    mart_crm_account.account_bdr_assigned_user_role,
    mart_crm_account.bdr_recycle_date,
    mart_crm_account.actively_working_start_date,
    opps_missing_link.dim_crm_opportunity_id,
    opps_missing_link.sdr_sao_id,
    opps_missing_link.bdr_first_order_sao_id,
    opps_missing_link.sales_accepted_opportunity_id,
    opps_missing_link.pipeline_opportunity_id,
    opps_missing_link.first_order_booked_opportunity_id,
    NULL                                                           AS mql_sales_accepted_opportunity_id,
    opps_missing_link.sdr_bdr_user_id,
    opps_missing_link.net_arr,
    opps_missing_link.first_order_booked_net_arr,
    opps_missing_link.pipeline_net_arr,
    opps_missing_link.pipeline_net_arr_qtd,
    opps_missing_link.bdr_stage_1_net_arr,
    opps_missing_link.bdr_stage_3_net_arr,
    opps_missing_link.net_arr_stage_1,
    opps_missing_link.xdr_net_arr_stage_1,
    opps_missing_link.xdr_net_arr_stage_3,
    opps_missing_link.sales_accepted_date,
    opps_missing_link.sales_accepted_fiscal_quarter_name,
    opps_missing_link.sao_day_of_fiscal_quarter,
    opps_missing_link.sao_day_of_fiscal_year,
    opps_missing_link.sao_fiscal_quarters_ago,
    opps_missing_link.pipeline_day_of_fiscal_quarter,
    opps_missing_link.pipeline_day_of_fiscal_year,
    opps_missing_link.pipeline_fiscal_quarters_ago,
    NULL                                                          AS min_mql_fiscal_quarters_ago,
    'Outbound'                                                    AS opportunity_source_type,
    opps_missing_link.stage_0_pending_acceptance_date,
    opps_missing_link.stage_0_pending_acceptance_month,
    opps_missing_link.stage_0_pending_acceptance_fiscal_quarter_name,
    opps_missing_link.stage_1_discovery_date,
    opps_missing_link.stage_1_discovery_month,
    opps_missing_link.stage_1_discovery_fiscal_quarter_name,
    opps_missing_link.stage_2_scoping_date,
    opps_missing_link.stage_2_scoping_month,
    opps_missing_link.stage_2_scoping_fiscal_quarter_name,
    opps_missing_link.stage_3_technical_evaluation_date,
    opps_missing_link.stage_3_technical_evaluation_month,
    opps_missing_link.stage_3_technical_evaluation_fiscal_quarter_name,
    opps_missing_link.pipeline_created_date,
    opps_missing_link.pipeline_created_month,
    opps_missing_link.pipeline_created_fiscal_quarter_name,
    opps_missing_link.pipeline_created_fiscal_year,
    opps_missing_link.days_in_1_discovery,
    opps_missing_link.days_in_sao,
    opps_missing_link.days_since_last_activity,
    opps_missing_link.sales_qualified_source_name,
    opps_missing_link.sdr_sqs_or_not,
    opps_missing_link.report_segment,
    opps_missing_link.report_geo,
    opps_missing_link.report_region,
    opps_missing_link.report_area,
    opps_missing_link.parent_crm_account_geo_pubsec_segment,
    opps_missing_link.report_role_level_1,
    opps_missing_link.report_role_level_2,
    opps_missing_link.report_role_level_3,
    opps_missing_link.pipe_council_grouping,
    opps_missing_link.deal_path_name,
    opps_missing_link.opp_created_date,
    opps_missing_link.close_date,
    opps_missing_link.close_day_of_fiscal_quarter,
    opps_missing_link.close_day_of_fiscal_year,
    opps_missing_link.close_fiscal_quarter_name,
    opps_missing_link.close_fiscal_quarters_ago,
    opps_missing_link.current_date_actual,
    opps_missing_link.current_day_of_fiscal_quarter,
    opps_missing_link.current_day_of_fiscal_year,
    opps_missing_link.activity_to_sao_days,
    opps_missing_link.order_type,
    opps_missing_link.order_type_grouped,
    opps_missing_link.new_logo_count,
    opps_missing_link.new_logo_count_snapshot,
    opps_missing_link.opportunity_category,
    opps_missing_link.stage_name,
    opps_missing_link.product_category,
    opps_missing_link.product_details,
    opps_missing_link.products_purchased,
    opps_missing_link.sales_dev_bdr_or_sdr,
    opps_missing_link.opportunity_sales_development_representative,
    opps_missing_link.opportunity_business_development_representative,
    opps_missing_link.is_sao,
    opps_missing_link.is_sales_dev_qualified_opportunity,
    opps_missing_link.is_sdr_first_order_booked_deal,
    opps_missing_link.is_booked_net_arr,
    opps_missing_link.is_net_arr_closed_deal,
    opps_missing_link.is_net_arr_pipeline_created,
    opps_missing_link.is_eligible_age_analysis,
    opps_missing_link.is_eligible_open_pipeline,
    opps_missing_link.is_actively_working_bdr_status AS activity_six_sense_account_buying_stage,
    opps_missing_link.six_sense_account_buying_stage AS activity_is_actively_worked_account,
    opportunity_snapshot_hierarchy.dim_crm_user_id                 AS sales_dev_rep_user_id,
    opportunity_snapshot_hierarchy.sales_dev_rep_role_name,
    opportunity_snapshot_hierarchy.sales_dev_rep_email,
    opportunity_snapshot_hierarchy.sales_dev_rep_user_full_name    AS sales_dev_rep_full_name,
    opportunity_snapshot_hierarchy.sales_dev_rep_title,
    opportunity_snapshot_hierarchy.sales_dev_rep_department,
    opportunity_snapshot_hierarchy.sales_dev_rep_team,
    opportunity_snapshot_hierarchy.sales_dev_rep_is_active,
    opportunity_snapshot_hierarchy.sales_dev_rep_user_role_level_1,
    opportunity_snapshot_hierarchy.sales_dev_rep_user_role_level_2,
    opportunity_snapshot_hierarchy.sales_dev_rep_user_role_level_3,
    opportunity_snapshot_hierarchy.crm_user_sales_segment,
    opportunity_snapshot_hierarchy.crm_user_geo,
    opportunity_snapshot_hierarchy.crm_user_region,
    opportunity_snapshot_hierarchy.crm_user_area,
    opportunity_snapshot_hierarchy.sales_dev_rep_employee_number,
    opportunity_snapshot_hierarchy.sales_dev_rep_direct_manager_id,
    opportunity_snapshot_hierarchy.sales_dev_rep_manager_full_name AS sales_dev_manager_full_name,
    opportunity_snapshot_hierarchy.sales_dev_manager_email,
    opportunity_snapshot_hierarchy.sales_dev_manager_employee_number,
    opportunity_snapshot_hierarchy.sales_dev_manager_user_role_name,
    opportunity_snapshot_hierarchy.sales_dev_leader_id,
    opportunity_snapshot_hierarchy.sales_dev_leader_user_role_name,
    opportunity_snapshot_hierarchy.sales_dev_rep_leader_full_name  AS sales_dev_leader_full_name,
    opportunity_snapshot_hierarchy.sales_dev_leader_employee_number,
    opportunity_snapshot_hierarchy.sales_dev_leader_email,
    NULL                                                           AS activity_sales_dev_rep_user_id,
    NULL                                                           AS activity_sales_dev_rep_role_name,
    NULL                                                           AS activity_sales_dev_rep_email,
    NULL                                                           AS activity_sales_dev_rep_full_name,
    NULL                                                           AS activity_sales_dev_manager_full_name,
    NULL                                                           AS activity_sales_dev_leader_full_name,
    NULL                                                           AS activity_sales_dev_rep_user_role_level_1,
    NULL                                                           AS activity_sales_dev_rep_user_role_level_2,
    NULL                                                           AS activity_sales_dev_rep_user_role_level_3
  FROM opps_missing_link
  LEFT JOIN dim_sales_dev_user_hierarchy AS opportunity_snapshot_hierarchy
    ON opps_missing_link.sdr_bdr_user_id = opportunity_snapshot_hierarchy.dim_crm_user_id
      AND opps_missing_link.stage_1_discovery_date = opportunity_snapshot_hierarchy.snapshot_date
  LEFT JOIN mart_crm_account
    ON opps_missing_link.dim_crm_account_id = mart_crm_account.dim_crm_account_id

)

SELECT *
FROM final
