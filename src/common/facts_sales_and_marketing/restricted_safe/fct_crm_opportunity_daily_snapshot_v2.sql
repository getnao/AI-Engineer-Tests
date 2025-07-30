{{ config(
    materialized="incremental",
    unique_key="crm_opportunity_snapshot_id",
    on_schema_change="sync_all_columns"
) }}

{{ simple_cte([
    ('prep_sales_qualified_source', 'prep_sales_qualified_source'),
    ('prep_order_type', 'prep_order_type'),
    ('prep_dr_partner_engagement', 'prep_dr_partner_engagement'),
    ('prep_deal_path', 'prep_deal_path'),
    ('prep_alliance_type', 'prep_alliance_type_scd'),
    ('prep_channel_type', 'prep_channel_type'),
    ('dim_date', 'dim_date'),
    ('prep_crm_user', 'prep_crm_user'),
    ('prep_crm_user_hierarchy', 'prep_crm_user_hierarchy'),
    ('prep_crm_account', 'prep_crm_account'),
    ('map_crm_account', 'map_crm_account'),
    ('prep_sales_segment', 'prep_sales_segment')
]) }},

prep_crm_opportunity AS (

    SELECT 
      *,
      -- We create the surrogate keys while only considering prep_crm_opportunity because there are ambiguous fields if we do so after joining to other models
      {{ dbt_utils.generate_surrogate_key(get_opportunity_flag_fields()) }}             AS dim_crm_opportunity_flags_sk,
      {{ dbt_utils.generate_surrogate_key(get_opportunity_command_plan_fields()) }}     AS dim_crm_command_plan_sk,
      {{ dbt_utils.generate_surrogate_key(get_opportunity_deal_fields()) }}             AS dim_crm_opportunity_deal_sk,
      {{ dbt_utils.generate_surrogate_key(get_opportunity_source_and_path_fields()) }}  AS dim_crm_opportunity_source_and_path_sk,
      {{ dbt_utils.generate_surrogate_key(get_opportunity_partner_fields()) }}          AS dim_crm_opportunity_partner_sk  
    FROM {{ref('prep_crm_opportunity')}}  
    WHERE is_live = 0 -- not live data because we rely on snapshots. Live data is in fct_crm_opportunity
    {% if is_incremental() %}

    AND prep_crm_opportunity.snapshot_date > (SELECT MAX(snapshot_date) FROM {{this}})

    {% endif %}
    
),

final AS (

  SELECT 
    -- Primary Key 
    prep_crm_opportunity.crm_opportunity_snapshot_id,

    -- Snapshot main fields
    prep_crm_opportunity.snapshot_id,
    prep_crm_opportunity.snapshot_date,
    prep_crm_opportunity.snapshot_month,
    prep_crm_opportunity.snapshot_fiscal_year,
    prep_crm_opportunity.snapshot_fiscal_quarter_name,
    prep_crm_opportunity.snapshot_fiscal_quarter_date,
    prep_crm_opportunity.snapshot_day_of_fiscal_quarter_normalised,
    prep_crm_opportunity.snapshot_day_of_fiscal_year_normalised,
    prep_crm_opportunity.snapshot_last_day_of_fiscal_quarter,

    -- Foreign Keys
    prep_crm_opportunity.dim_crm_opportunity_id,    
    prep_crm_opportunity.merged_opportunity_id                                          AS merged_crm_opportunity_id,
    prep_crm_opportunity.merged_opportunity_id,
    prep_crm_opportunity.dim_parent_crm_opportunity_id,
    prep_crm_opportunity.duplicate_opportunity_id,
    prep_crm_opportunity.dim_crm_account_id,
    prep_crm_opportunity.dim_parent_crm_account_id,
    prep_crm_opportunity.dim_crm_user_id,
    prep_crm_opportunity.dim_crm_person_id,
    prep_crm_opportunity.sfdc_contact_id,
    prep_crm_opportunity.crm_sales_dev_rep_id,
    prep_crm_opportunity.crm_business_dev_rep_id,
    prep_crm_opportunity.record_type_id,
    prep_crm_opportunity.ssp_id,
    prep_crm_opportunity.ga_client_id,
    prep_crm_opportunity.dim_crm_current_account_set_hierarchy_sk,
    prep_crm_opportunity.dim_crm_opp_owner_stamped_hierarchy_sk,
    prep_crm_opportunity.contract_reset_opportunity_id,
    prep_crm_opportunity.invoice_number,                                                                                                                                                                                                                                       
    {{ get_keyed_nulls('prep_sales_qualified_source.dim_sales_qualified_source_id') }}   AS dim_sales_qualified_source_id,
    {{ get_keyed_nulls('prep_order_type.dim_order_type_id') }}                           AS dim_order_type_id,
    {{ get_keyed_nulls('prep_order_type_current.dim_order_type_id') }}                   AS dim_order_type_current_id,
    {{ get_keyed_nulls('prep_dr_partner_engagement.dim_dr_partner_engagement_id') }}     AS dim_dr_partner_engagement_id,
    {{ get_keyed_nulls('prep_alliance_type.dim_alliance_type_id') }}                     AS dim_alliance_type_id,
    {{ get_keyed_nulls('prep_alliance_type_current.dim_alliance_type_id') }}             AS dim_alliance_type_current_id,
    {{ get_keyed_nulls('prep_channel_type.dim_channel_type_id') }}                       AS dim_channel_type_id,
    {{ get_keyed_nulls('prep_deal_path.dim_deal_path_id') }}                             AS dim_deal_path_id,
    {{ get_keyed_nulls('map_crm_account.dim_parent_sales_segment_id,prep_sales_segment.dim_sales_segment_id') }}              AS dim_parent_sales_segment_id,
    map_crm_account.dim_parent_sales_territory_id,
    map_crm_account.dim_parent_industry_id,
    {{ get_keyed_nulls('map_crm_account.dim_account_sales_segment_id,prep_sales_segment.dim_sales_segment_id') }}             AS dim_account_sales_segment_id,
    map_crm_account.dim_account_sales_territory_id,
    map_crm_account.dim_account_industry_id,
    map_crm_account.dim_account_location_country_id,
    map_crm_account.dim_account_location_region_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_hierarchy_id') }}           AS dim_crm_opp_owner_user_hierarchy_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_business_unit_id') }}       AS dim_crm_opp_owner_business_unit_stamped_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_sales_segment_id') }}       AS dim_crm_opp_owner_sales_segment_stamped_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_geo_id') }}                 AS dim_crm_opp_owner_geo_stamped_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_region_id') }}              AS dim_crm_opp_owner_region_stamped_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_area_id') }}                AS dim_crm_opp_owner_area_stamped_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_name_id') }}           AS dim_crm_opp_owner_role_name_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_level_1_id') }}        AS dim_crm_opp_owner_role_level_1_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_level_2_id') }}        AS dim_crm_opp_owner_role_level_2_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_level_3_id') }}        AS dim_crm_opp_owner_role_level_3_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_level_4_id') }}        AS dim_crm_opp_owner_role_level_4_id,
    {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_level_5_id') }}        AS dim_crm_opp_owner_role_level_5_id,
    {{ get_keyed_nulls('opp_owner.dim_crm_user_hierarchy_sk') }}                        AS dim_crm_user_hierarchy_live_sk,
    {{ get_keyed_nulls('opp_owner.dim_crm_user_business_unit_id') }}                    AS dim_crm_user_business_unit_id,
    {{ get_keyed_nulls('opp_owner.dim_crm_user_sales_segment_id') }}                    AS dim_crm_user_sales_segment_id,
    {{ get_keyed_nulls('opp_owner.dim_crm_user_geo_id') }}                              AS dim_crm_user_geo_id,
    {{ get_keyed_nulls('opp_owner.dim_crm_user_region_id') }}                           AS dim_crm_user_region_id,
    {{ get_keyed_nulls('opp_owner.dim_crm_user_area_id') }}                             AS dim_crm_user_area_id,
    {{ get_keyed_nulls('account_owner.dim_crm_user_hierarchy_sk') }}                    AS dim_crm_user_hierarchy_account_user_sk,
    {{ get_keyed_nulls('account_owner.dim_crm_user_business_unit_id') }}                AS dim_crm_account_user_business_unit_id,
    {{ get_keyed_nulls('account_owner.dim_crm_user_sales_segment_id') }}                AS dim_crm_account_user_sales_segment_id,
    {{ get_keyed_nulls('account_owner.dim_crm_user_geo_id') }}                          AS dim_crm_account_user_geo_id,
    {{ get_keyed_nulls('account_owner.dim_crm_user_region_id') }}                       AS dim_crm_account_user_region_id,
    {{ get_keyed_nulls('account_owner.dim_crm_user_area_id') }}                         AS dim_crm_account_user_area_id,
    {{ get_keyed_nulls('account_owner.dim_crm_user_role_name_id') }}                    AS dim_crm_account_user_role_name_id,
    {{ get_keyed_nulls('account_owner.dim_crm_user_role_level_1_id') }}                 AS dim_crm_account_user_role_level_1_id,
    {{ get_keyed_nulls('account_owner.dim_crm_user_role_level_2_id') }}                 AS dim_crm_account_user_role_level_2_id,
    {{ get_keyed_nulls('account_owner.dim_crm_user_role_level_3_id') }}                 AS dim_crm_account_user_role_level_3_id,
    {{ get_keyed_nulls('account_owner.dim_crm_user_role_level_4_id') }}                 AS dim_crm_account_user_role_level_4_id,
    {{ get_keyed_nulls('account_owner.dim_crm_user_role_level_5_id') }}                 AS dim_crm_account_user_role_level_5_id,
    prep_crm_opportunity.dim_sales_qualified_source_id_simplified,
    prep_crm_opportunity.dim_crm_opportunity_flags_sk,
    prep_crm_opportunity.dim_crm_command_plan_sk,
    prep_crm_opportunity.dim_crm_opportunity_deal_sk,
    prep_crm_opportunity.dim_crm_opportunity_source_and_path_sk,
    prep_crm_opportunity.dim_crm_opportunity_partner_sk,
    
    IFF(close_fiscal_year < dim_date.current_fiscal_year,
        dim_crm_account_user_sales_segment_id, dim_crm_opp_owner_sales_segment_stamped_id)     AS dim_crm_current_account_set_sales_segment_id,
    IFF(close_fiscal_year < dim_date.current_fiscal_year,
        dim_crm_account_user_geo_id, dim_crm_opp_owner_geo_stamped_id)                         AS dim_crm_current_account_set_geo_id,
    IFF(close_fiscal_year < dim_date.current_fiscal_year,
        dim_crm_account_user_region_id, dim_crm_opp_owner_region_stamped_id)                   AS dim_crm_current_account_set_region_id,
    IFF(close_fiscal_year < dim_date.current_fiscal_year,
        dim_crm_account_user_area_id, dim_crm_opp_owner_area_stamped_id)                       AS dim_crm_current_account_set_area_id,
    IFF(close_fiscal_year < dim_date.current_fiscal_year,
        dim_crm_account_user_business_unit_id, dim_crm_opp_owner_business_unit_stamped_id)     AS dim_crm_current_account_set_business_unit_id,
    IFF(close_fiscal_year < dim_date.current_fiscal_year,
        dim_crm_account_user_role_name_id, dim_crm_opp_owner_role_name_id)                     AS dim_crm_current_account_set_role_name_id,
    IFF(close_fiscal_year < dim_date.current_fiscal_year,
        dim_crm_account_user_role_level_1_id, dim_crm_opp_owner_role_level_1_id)               AS dim_crm_current_account_set_role_level_1_id,
    IFF(close_fiscal_year < dim_date.current_fiscal_year,
        dim_crm_account_user_role_level_2_id, dim_crm_opp_owner_role_level_2_id)               AS dim_crm_current_account_set_role_level_2_id,
    IFF(close_fiscal_year < dim_date.current_fiscal_year,
        dim_crm_account_user_role_level_3_id, dim_crm_opp_owner_role_level_3_id)               AS dim_crm_current_account_set_role_level_3_id,
    IFF(close_fiscal_year < dim_date.current_fiscal_year,
        dim_crm_account_user_role_level_4_id, dim_crm_opp_owner_role_level_4_id)               AS dim_crm_current_account_set_role_level_4_id,
    IFF(close_fiscal_year < dim_date.current_fiscal_year,
        dim_crm_account_user_role_level_5_id, dim_crm_opp_owner_role_level_5_id)               AS dim_crm_current_account_set_role_level_5_id,

    --live fields
    prep_crm_opportunity.sales_qualified_source,
    prep_crm_opportunity.sales_qualified_source_live,
    prep_crm_opportunity.sales_qualified_source_grouped,
    prep_crm_opportunity.sales_qualified_source_grouped_live,
    prep_crm_opportunity.is_edu_oss_live,
    prep_crm_opportunity.opportunity_category_live,
    prep_crm_opportunity.is_jihu_account_live,
    prep_crm_opportunity.deal_path,
    prep_crm_opportunity.deal_path_live,
    prep_crm_opportunity.parent_crm_account_geo_live,
    prep_crm_opportunity.order_type,
    prep_crm_opportunity.order_type_live,
    prep_crm_opportunity.order_type_grouped,
    prep_crm_opportunity.order_type_grouped_live,
    prep_crm_opportunity.subscription_type,
    prep_crm_opportunity.close_fiscal_quarter_name_live,

    -- Key Process Dates
    prep_crm_opportunity.created_date_id,
    prep_crm_opportunity.created_date,
    prep_crm_opportunity.sales_accepted_date_id,
    prep_crm_opportunity.sales_accepted_date,
    prep_crm_opportunity.sales_qualified_date_id,
    prep_crm_opportunity.close_date_id,
    prep_crm_opportunity.close_date,
    prep_crm_opportunity.arr_created_date_id,
    prep_crm_opportunity.arr_created_date,
    prep_crm_opportunity.last_activity_date_id,
    prep_crm_opportunity.sales_last_activity_date_id,
    prep_crm_opportunity.subscription_start_date_id,
    prep_crm_opportunity.subscription_end_date_id,
    {{ get_date_id('subscription_renewal_date') }}                                     AS subscription_renewal_date_id,
    {{ get_date_id('quote_start_date') }}                                              AS quote_start_date_id,

    -- Stage info 
    prep_crm_opportunity.stage_name,
    prep_crm_opportunity.stage_name_3plus,
    prep_crm_opportunity.stage_name_4plus,
    prep_crm_opportunity.stage_category,

    prep_crm_opportunity.is_stage_1_plus,
    prep_crm_opportunity.is_stage_3_plus,
    prep_crm_opportunity.is_stage_4_plus,

    -- Stage Progression Dates
    prep_crm_opportunity.stage_0_pending_acceptance_date_id,
    prep_crm_opportunity.stage_1_discovery_date_id,
    prep_crm_opportunity.stage_2_scoping_date_id,
    prep_crm_opportunity.stage_3_technical_evaluation_date_id,
    prep_crm_opportunity.stage_4_proposal_date_id,
    prep_crm_opportunity.stage_5_negotiating_date_id,
    prep_crm_opportunity.stage_6_awaiting_signature_date_id,
    prep_crm_opportunity.stage_6_closed_won_date_id,
    prep_crm_opportunity.stage_6_closed_lost_date_id,

    prep_crm_opportunity.close_fiscal_quarter_date,
    prep_crm_opportunity.created_fiscal_quarter_date,
    prep_crm_opportunity.arr_created_fiscal_quarter_date,


    -- Stage Duration Metrics
    prep_crm_opportunity.days_in_0_pending_acceptance,
    prep_crm_opportunity.days_in_1_discovery,
    prep_crm_opportunity.days_in_2_scoping,
    prep_crm_opportunity.days_in_3_technical_evaluation,
    prep_crm_opportunity.days_in_4_proposal,
    prep_crm_opportunity.days_in_5_negotiating,
    prep_crm_opportunity.days_in_stage,
    prep_crm_opportunity.cycle_time_in_days,
    prep_crm_opportunity.calculated_age_in_days,
    prep_crm_opportunity.days_in_sao,
    prep_crm_opportunity.days_since_last_activity,

    -- Technical Evaluation
    prep_crm_opportunity.sa_tech_evaluation_close_status,
    prep_crm_opportunity.sa_tech_evaluation_end_date,
    prep_crm_opportunity.sa_tech_evaluation_start_date,
    prep_crm_opportunity.technical_evaluation_date_id,

    -- PTC related fields
    prep_crm_opportunity.ptc_predicted_arr,
    prep_crm_opportunity.ptc_predicted_renewal_risk_category,

    -- Financial Amounts
    prep_crm_opportunity.amount,
    prep_crm_opportunity.recurring_amount,
    prep_crm_opportunity.true_up_amount,
    prep_crm_opportunity.proserv_amount,
    prep_crm_opportunity.other_non_recurring_amount,
    prep_crm_opportunity.renewal_amount,
    prep_crm_opportunity.total_contract_value,
    prep_crm_opportunity.professional_services_value,
    prep_crm_opportunity.calculated_discount,

    -- ARR Metrics
    prep_crm_opportunity.arr,
    prep_crm_opportunity.arr_basis,
    prep_crm_opportunity.created_arr,
    prep_crm_opportunity.raw_net_arr,
    prep_crm_opportunity.net_arr,
    prep_crm_opportunity.net_arr_stage_1,
    prep_crm_opportunity.enterprise_agile_planning_net_arr,
    prep_crm_opportunity.duo_net_arr,
    prep_crm_opportunity.vsa_start_date_net_arr,
    prep_crm_opportunity.open_1plus_net_arr,
    prep_crm_opportunity.open_3plus_net_arr,
    prep_crm_opportunity.open_4plus_net_arr,
    prep_crm_opportunity.booked_net_arr,
    prep_crm_opportunity.positive_open_net_arr,
    prep_crm_opportunity.positive_booked_net_arr,
    prep_crm_opportunity.closed_net_arr,
    prep_crm_opportunity.created_and_won_same_quarter_net_arr,

    prep_crm_opportunity.landing_quarter_relative_to_arr_created_date,
    prep_crm_opportunity.landing_quarter_relative_to_snapshot_date,
    prep_crm_opportunity.snapshot_to_close_diff,
    prep_crm_opportunity.arr_created_to_close_diff,
    CASE 
      WHEN prep_crm_opportunity.arr_created_date >= dim_date.current_first_day_of_fiscal_quarter
        THEN prep_crm_opportunity.close_fiscal_quarter_name_live
      ELSE prep_crm_opportunity.close_fiscal_quarter_name
    END AS close_fiscal_quarter_name_for_pipegen_grouping,

    prep_crm_opportunity.created_arr_in_snapshot_quarter,
    prep_crm_opportunity.closed_won_opps_in_snapshot_quarter,
    prep_crm_opportunity.closed_opps_in_snapshot_quarter,
    prep_crm_opportunity.booked_net_arr_in_snapshot_quarter,
    prep_crm_opportunity.created_deals_in_snapshot_quarter,
    prep_crm_opportunity.cycle_time_in_days_in_snapshot_quarter,
    prep_crm_opportunity.booked_deal_count_in_snapshot_quarter,
    prep_crm_opportunity.open_1plus_net_arr_in_snapshot_quarter,
    prep_crm_opportunity.open_3plus_net_arr_in_snapshot_quarter,
    prep_crm_opportunity.open_4plus_net_arr_in_snapshot_quarter,
    prep_crm_opportunity.open_1plus_deal_count_in_snapshot_quarter,
    prep_crm_opportunity.open_3plus_deal_count_in_snapshot_quarter,
    prep_crm_opportunity.open_4plus_deal_count_in_snapshot_quarter,
    prep_crm_opportunity.positive_booked_deal_count_in_snapshot_quarter,
    prep_crm_opportunity.positive_booked_net_arr_in_snapshot_quarter,
    prep_crm_opportunity.positive_open_deal_count_in_snapshot_quarter,
    prep_crm_opportunity.positive_open_net_arr_in_snapshot_quarter,
    prep_crm_opportunity.closed_deals_in_snapshot_quarter,
    prep_crm_opportunity.closed_net_arr_in_snapshot_quarter,
    prep_crm_opportunity.created_in_snapshot_quarter_net_arr,
    prep_crm_opportunity.created_in_snapshot_quarter_deal_count,
    prep_crm_opportunity.booked_ps_value_in_snapshot_quarter,

    -- First order
    prep_crm_opportunity.first_order_open_1plus_saos,
    prep_crm_opportunity.first_order_open_3plus_saos,
    prep_crm_opportunity.first_order_open_4plus_saos,
    prep_crm_opportunity.first_order_saos_generated,
    prep_crm_opportunity.first_order_booked_deals,
    prep_crm_opportunity.first_order_open_1plus_pipeline,
    prep_crm_opportunity.first_order_open_3plus_pipeline,
    prep_crm_opportunity.first_order_open_4plus_pipeline,
    prep_crm_opportunity.first_order_pipeline_generated,
    prep_crm_opportunity.first_order_bookings,
    prep_crm_opportunity.first_order_closed_deals,
    prep_crm_opportunity.first_order_closed_opps,
    prep_crm_opportunity.first_order_closed_won_opps,
    prep_crm_opportunity.first_order_closed_net_arr,
    prep_crm_opportunity.sales_dev_first_order_booked_deal_count,

    -- First order in snapshot quarter
    prep_crm_opportunity.first_order_open_1plus_saos_in_snapshot_quarter,
    prep_crm_opportunity.first_order_open_3plus_saos_in_snapshot_quarter,
    prep_crm_opportunity.first_order_open_4plus_saos_in_snapshot_quarter,
    prep_crm_opportunity.first_order_saos_generated_in_snapshot_quarter,
    prep_crm_opportunity.first_order_booked_deals_in_snapshot_quarter,
    prep_crm_opportunity.first_order_open_1plus_pipeline_in_snapshot_quarter,
    prep_crm_opportunity.first_order_open_3plus_pipeline_in_snapshot_quarter,
    prep_crm_opportunity.first_order_open_4plus_pipeline_in_snapshot_quarter,
    prep_crm_opportunity.first_order_pipeline_generated_in_snapshot_quarter,
    prep_crm_opportunity.first_order_bookings_in_snapshot_quarter,
    prep_crm_opportunity.first_order_closed_deals_in_snapshot_quarter,
    prep_crm_opportunity.first_order_closed_opps_in_snapshot_quarter,
    prep_crm_opportunity.first_order_closed_won_opps_in_snapshot_quarter,
    prep_crm_opportunity.first_order_closed_net_arr_in_snapshot_quarter,

    -- Deal & Logo Counts
    prep_crm_opportunity.new_logo_count,
    prep_crm_opportunity.calculated_deal_count,
    prep_crm_opportunity.open_1plus_deal_count,
    prep_crm_opportunity.open_3plus_deal_count,
    prep_crm_opportunity.open_4plus_deal_count,
    prep_crm_opportunity.booked_deal_count,
    prep_crm_opportunity.positive_booked_deal_count,
    prep_crm_opportunity.positive_open_deal_count,
    prep_crm_opportunity.closed_deals,
    prep_crm_opportunity.closed_won_opps,
    prep_crm_opportunity.closed_opps,
    prep_crm_opportunity.created_deals,
    prep_crm_opportunity.booked_ps_value,

    -- Churn AND Contraction Metrics
    prep_crm_opportunity.churned_contraction_deal_count,
    prep_crm_opportunity.churned_contraction_net_arr,
    prep_crm_opportunity.booked_churned_contraction_deal_count,
    prep_crm_opportunity.booked_churned_contraction_net_arr,
    prep_crm_opportunity.forecasted_churn_for_clari,

    -- Incremental AND Ratio Metrics
    prep_crm_opportunity.incremental_acv                                                                    AS iacv,
    prep_crm_opportunity.net_incremental_acv                                                                AS net_iacv,
    prep_crm_opportunity.incremental_acv, -- adding unaliased fields for some reports
    prep_crm_opportunity.net_incremental_acv, -- adding unaliased fields for some reports
    prep_crm_opportunity.opportunity_based_iacv_to_net_arr_ratio,
    prep_crm_opportunity.segment_order_type_iacv_to_net_arr_ratio,
    prep_crm_opportunity.calculated_from_ratio_net_arr,
    prep_crm_opportunity.weighted_linear_iacv,
    prep_crm_opportunity.closed_buckets,
    prep_crm_opportunity.count_campaigns,
    prep_crm_opportunity.count_crm_attribution_touchpoints,

  -- SA Specific Metrics
    prep_crm_opportunity.number_of_sa_activity_tasks,

    -- XDR Specific Metrics
    prep_crm_opportunity.xdr_net_arr_stage_1,
    prep_crm_opportunity.xdr_net_arr_stage_3,

    -- Clari Specific Fields
    prep_crm_opportunity.won_arr_basis_for_clari,
    prep_crm_opportunity.arr_basis_for_clari,
    prep_crm_opportunity.override_arr_basis_clari,  

    -- Other
    prep_crm_opportunity.potential_seat_count,
    prep_crm_opportunity.edu_services_value,
    prep_crm_opportunity.investment_services_value,
    prep_crm_opportunity.probability

  FROM prep_crm_opportunity
  LEFT JOIN prep_sales_qualified_source
    ON prep_crm_opportunity.sales_qualified_source = prep_sales_qualified_source.sales_qualified_source_name
  LEFT JOIN prep_order_type 
    ON prep_crm_opportunity.order_type = prep_order_type.order_type_name
  LEFT JOIN prep_order_type AS prep_order_type_current
    ON prep_crm_opportunity.order_type_current = prep_order_type_current.order_type_name
  LEFT JOIN prep_dr_partner_engagement
    ON prep_crm_opportunity.dr_partner_engagement = prep_dr_partner_engagement.dr_partner_engagement_name
  LEFT JOIN prep_alliance_type
    ON prep_crm_opportunity.alliance_type = prep_alliance_type.alliance_type_name
  LEFT JOIN prep_alliance_type AS prep_alliance_type_current
    ON prep_crm_opportunity.alliance_type_current = prep_alliance_type_current.alliance_type_name
  LEFT JOIN prep_channel_type
    ON prep_crm_opportunity.channel_type = prep_channel_type.channel_type_name
  LEFT JOIN dim_date
    ON dim_date.date_id = prep_crm_opportunity.close_date_id
  LEFT JOIN prep_crm_user_hierarchy
    ON prep_crm_opportunity.dim_crm_opp_owner_stamped_hierarchy_sk = prep_crm_user_hierarchy.dim_crm_user_hierarchy_sk
  LEFT JOIN prep_crm_account
    ON prep_crm_opportunity.dim_crm_account_id = prep_crm_account.dim_crm_account_id
  LEFT JOIN prep_crm_user AS opp_owner
    ON prep_crm_opportunity.dim_crm_user_id = opp_owner.dim_crm_user_id
  LEFT JOIN prep_crm_user AS account_owner
    ON prep_crm_account.dim_crm_user_id = account_owner.dim_crm_user_id
  LEFT JOIN map_crm_account
    ON prep_crm_opportunity.dim_crm_account_id = map_crm_account.dim_crm_account_id
  LEFT JOIN prep_deal_path
    ON prep_crm_opportunity.deal_path = prep_deal_path.deal_path_name
  LEFT JOIN prep_sales_segment
    ON prep_crm_opportunity.sales_segment = prep_sales_segment.sales_segment_name

)

SELECT *
FROM final
