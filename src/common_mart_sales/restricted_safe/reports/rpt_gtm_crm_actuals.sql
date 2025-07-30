{{ config(
    tags=["six_hourly"]
) }}

{{ simple_cte([
    ('dim_date','dim_date'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('dim_crm_opportunity_flags', 'dim_crm_opportunity_flags')
]) }},

fct_crm_opportunity AS (

  SELECT *
  FROM {{ ref('fct_crm_opportunity', v=2) }}

),

created_actuals AS (
  SELECT
    fct_crm_opportunity.dim_crm_opportunity_id,
    fct_crm_opportunity.dim_order_type_id,
    fct_crm_opportunity.dim_sales_qualified_source_id,
    fct_crm_opportunity.dim_crm_current_account_set_hierarchy_sk,
    fct_crm_opportunity.arr_created_date                                    AS actual_date,
    CASE WHEN dim_crm_opportunity_flags.is_net_arr_pipeline_created = TRUE
        AND fct_crm_opportunity.arr_created_date IS NOT NULL
        THEN fct_crm_opportunity.net_arr
    END                                                                     AS net_arr_pipeline_created,
    IFF(
      dim_crm_opportunity_flags.fpa_master_bookings_flag = TRUE,
      fct_crm_opportunity.net_arr, NULL
    )                                                                       AS sao_net_arr,
    IFF(dim_crm_opportunity_flags.is_sao = TRUE, 1, NULL)                   AS sao_opportunity_id_count,
    IFF(fct_crm_opportunity.new_logo_count != 0, new_logo_count, NULL)      AS pipegen_new_logos,
    NULL                                                                    AS open_new_logos,
    NULL                                                                    AS first_order_open_1plus_saos,
    NULL                                                                    AS first_order_open_3plus_saos,
    NULL                                                                    AS open_1plus_net_arr,
    NULL                                                                    AS open_3plus_net_arr,
    NULL                                                                    AS first_order_open_1plus_pipeline,
    NULL                                                                    AS first_order_open_3plus_pipeline,
    NULL                                                                    AS net_arr_closed, --to be deprecated
    NULL                                                                    AS booked_net_arr,
    NULL                                                                    AS new_logo_count_closed, -- to be deprecated
    NULL                                                                    AS first_order_booked_deals,
    NULL                                                                    AS deal_ids_count, --to be deprecated
    NULL                                                                    AS booked_deal_count
  FROM fct_crm_opportunity
  LEFT JOIN dim_crm_opportunity_flags
    ON fct_crm_opportunity.dim_crm_opportunity_flags_sk = dim_crm_opportunity_flags.dim_crm_opportunity_flags_sk
  WHERE dim_crm_opportunity_flags.is_net_arr_pipeline_created = TRUE
    AND fct_crm_opportunity.arr_created_date_id IS NOT NULL
),

closed_actuals AS (
  SELECT
    fct_crm_opportunity.dim_crm_opportunity_id,
    fct_crm_opportunity.dim_order_type_id,
    fct_crm_opportunity.dim_sales_qualified_source_id,
    fct_crm_opportunity.dim_crm_current_account_set_hierarchy_sk,
    fct_crm_opportunity.close_date                                    AS actual_date,
    NULL                                                              AS net_arr_pipeline_created,
    NULL                                                              AS sao_net_arr,
    NULL                                                              AS sao_opportunity_id_count,
    NULL                                                              AS pipegen_new_logos,
    CASE 
      WHEN fct_crm_opportunity.new_logo_count != 0
        AND dim_crm_opportunity_flags.is_eligible_open_pipeline = TRUE
        THEN new_logo_count
    END                                                              AS open_new_logos, -- to be deprecated
    fct_crm_opportunity.first_order_open_1plus_saos,  -- we categorize open deals by when they are projected to close, so open logos belongs in this CTE/
    fct_crm_opportunity.first_order_open_3plus_saos,
    fct_crm_opportunity.open_1plus_net_arr,
    fct_crm_opportunity.open_3plus_net_arr,
    fct_crm_opportunity.first_order_open_1plus_pipeline,
    fct_crm_opportunity.first_order_open_3plus_pipeline,
    CASE
      WHEN dim_crm_opportunity_flags.fpa_master_bookings_flag = TRUE
        THEN fct_crm_opportunity.booked_net_arr
    END                                                               AS net_arr_closed, --to be deprecated
    fct_crm_opportunity.booked_net_arr,
    CASE
      WHEN dim_crm_opportunity_flags.is_new_logo_first_order = TRUE
        AND dim_crm_opportunity_flags.fpa_master_bookings_flag = TRUE
        THEN fct_crm_opportunity.new_logo_count
    END                                                               AS new_logo_count_closed, -- to be deprecated
    fct_crm_opportunity.first_order_booked_deals,
    CASE
      WHEN fct_crm_opportunity.new_logo_count >= 0
        AND dim_crm_opportunity_flags.fpa_master_bookings_flag = TRUE
        THEN 1
      WHEN fct_crm_opportunity.new_logo_count = -1 
        AND dim_crm_opportunity_flags.fpa_master_bookings_flag = TRUE
        THEN -1
    END                                                               AS deal_ids_count, -- to be deprecated
    fct_crm_opportunity.booked_deal_count
  FROM fct_crm_opportunity
  LEFT JOIN dim_crm_opportunity_flags
    ON fct_crm_opportunity.dim_crm_opportunity_flags_sk = dim_crm_opportunity_flags.dim_crm_opportunity_flags_sk
),

--union
unioned_actuals AS (
  
  SELECT *
  FROM created_actuals

  UNION

  SELECT *
  FROM closed_actuals

),

main_actuals AS (
  SELECT
    unioned_actuals.dim_crm_opportunity_id,
    unioned_actuals.dim_order_type_id,
    unioned_actuals.dim_sales_qualified_source_id,
    unioned_actuals.dim_crm_current_account_set_hierarchy_sk,
    unioned_actuals.actual_date,
    dim_date.date_id                         AS actual_date_id,
    mart_crm_opportunity.product_category,
    mart_crm_opportunity.deal_size,
    mart_crm_opportunity.new_logo_count,
    mart_crm_opportunity.is_new_logo_first_order,
    dim_date.fiscal_quarter_name_fy,
    dim_date.fiscal_year,
    unioned_actuals.net_arr_pipeline_created,
    unioned_actuals.net_arr_closed, --to be deprecated
    unioned_actuals.booked_net_arr,
    unioned_actuals.new_logo_count_closed, -- to be deprecated
    unioned_actuals.first_order_booked_deals,
    unioned_actuals.deal_ids_count, -- to be deprecated
    unioned_actuals.booked_deal_count,
    unioned_actuals.sao_net_arr,
    unioned_actuals.sao_opportunity_id_count AS sao_count,
    unioned_actuals.pipegen_new_logos,
    unioned_actuals.open_new_logos,
    unioned_actuals.first_order_open_1plus_saos,
    unioned_actuals.first_order_open_1plus_pipeline,
    unioned_actuals.first_order_open_3plus_pipeline,
    -- closing this quarter
    IFF(
      dim_date.first_day_of_fiscal_quarter = dim_date.current_first_day_of_fiscal_quarter, 
      unioned_actuals.first_order_open_1plus_saos, NULL
    )                                        AS first_order_open_1plus_saos_closing_current_fiscal_quarter,
    IFF(
      dim_date.first_day_of_fiscal_quarter = dim_date.current_first_day_of_fiscal_quarter, 
      unioned_actuals.first_order_open_3plus_saos, NULL
    )                                        AS first_order_open_3plus_saos_closing_current_fiscal_quarter,
    IFF(
      dim_date.first_day_of_fiscal_quarter = dim_date.current_first_day_of_fiscal_quarter, 
      unioned_actuals.first_order_open_1plus_pipeline, NULL
    )                                        AS first_order_open_1plus_pipeline_closing_current_fiscal_quarter,
    IFF(
      dim_date.first_day_of_fiscal_quarter = dim_date.current_first_day_of_fiscal_quarter, 
      unioned_actuals.first_order_open_3plus_pipeline, NULL
    )                                        AS first_order_open_3plus_pipeline_closing_current_fiscal_quarter,
    -- closing this year
    IFF(
      dim_date.first_day_of_fiscal_year = dim_date.current_first_day_of_fiscal_year,
      unioned_actuals.first_order_open_1plus_saos, NULL
    )                                        AS first_order_open_1plus_saos_closing_current_fiscal_year,
    IFF(
      dim_date.first_day_of_fiscal_year = dim_date.current_first_day_of_fiscal_year,
      unioned_actuals.first_order_open_3plus_saos, NULL
    )                                        AS first_order_open_3plus_saos_closing_current_fiscal_year,
    IFF(
      dim_date.first_day_of_fiscal_year = dim_date.current_first_day_of_fiscal_year,
      unioned_actuals.first_order_open_1plus_pipeline, NULL
    )                                        AS first_order_open_1plus_pipeline_closing_current_fiscal_year,
      IFF(
      dim_date.first_day_of_fiscal_year = dim_date.current_first_day_of_fiscal_year,
      unioned_actuals.first_order_open_3plus_pipeline, NULL
    )                                        AS first_order_open_3plus_pipeline_closing_current_fiscal_year
  FROM unioned_actuals
  LEFT JOIN dim_date
    ON unioned_actuals.actual_date = dim_date.date_actual
  LEFT JOIN mart_crm_opportunity
    ON unioned_actuals.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
  WHERE dim_date.fiscal_year > 2019
  ORDER BY 1 DESC
)

SELECT
  *
FROM main_actuals