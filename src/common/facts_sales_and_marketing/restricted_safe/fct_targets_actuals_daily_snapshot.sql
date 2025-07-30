{{ simple_cte([
    ('targets', 'fct_sales_funnel_target_pivoted'),
    ('prep_date', 'prep_date')
]) }},

fct_crm_opportunity AS (

  SELECT *
  FROM {{ ref('fct_crm_opportunity', v=2) }}

),

actuals AS (

  SELECT *
  FROM {{ ref('fct_crm_opportunity_daily_snapshot', v=2) }}

),

max_snapshot_date AS (

  SELECT MAX(snapshot_date) AS max_snapshot_date_actuals
  FROM actuals
  WHERE snapshot_date >= DATEADD('day', -3, CURRENT_DATE())

),

live_actuals AS (

  /* 
    Grab final numbers for the quarter from the live data to ensure
    we capture deals that are closed on the last day of the quarter
  */
  SELECT
    fct_crm_opportunity.*,
    close_date.first_day_of_fiscal_quarter                                                               AS close_fiscal_quarter_date,
    close_date.fiscal_quarter_name_fy                                                                    AS close_fiscal_quarter_name
  FROM fct_crm_opportunity
  INNER JOIN prep_date AS close_date
    ON fct_crm_opportunity.close_date_id = close_date.date_id
  WHERE close_fiscal_quarter_date < current_first_day_of_fiscal_quarter

),

total_targets AS (

  SELECT
    prep_date.fiscal_quarter_name_fy                                                                     AS fiscal_quarter_name,
    prep_date.first_day_of_fiscal_quarter                                                                AS fiscal_quarter_date,
    targets.dim_crm_user_hierarchy_sk                                                                    AS dim_crm_current_account_set_hierarchy_sk,
    targets.dim_sales_qualified_source_id,
    targets.dim_order_type_id,
    SUM(targets.net_arr_daily_allocated_target)                                                          AS net_arr_total_quarter_target,
    SUM(targets.net_arr_pipeline_created_daily_allocated_target)                                         AS pipeline_created_total_quarter_target,
    SUM(targets.ps_value_daily_allocated_target)                                                         AS ps_value_total_quarter_target,
    SUM(new_logos_daily_allocated_target)                                                                AS new_logos_total_quarter_target,
    SUM(saos_daily_allocated_target)                                                                     AS saos_total_quarter_target
  FROM targets
  LEFT JOIN prep_date
    ON targets.target_date_id = prep_date.date_id
  {{ dbt_utils.group_by(n=5) }}

),

daily_actuals AS (

  SELECT
    actuals.snapshot_fiscal_quarter_name,
    actuals.snapshot_fiscal_quarter_date,
    actuals.snapshot_date,
    actuals.snapshot_id,
    actuals.dim_crm_current_account_set_hierarchy_sk,
    actuals.dim_sales_qualified_source_id,
    actuals.dim_order_type_id,
    SUM(booked_net_arr_in_snapshot_quarter)                                                              AS booked_net_arr_in_snapshot_quarter,
    SUM(open_1plus_net_arr_in_snapshot_quarter)                                                          AS open_1plus_net_arr_in_snapshot_quarter,
    SUM(open_3plus_net_arr_in_snapshot_quarter)                                                          AS open_3plus_net_arr_in_snapshot_quarter,
    SUM(open_4plus_net_arr_in_snapshot_quarter)                                                          AS open_4plus_net_arr_in_snapshot_quarter,
    SUM(created_arr_in_snapshot_quarter)                                                                 AS created_arr_in_snapshot_quarter,
    SUM(first_order_booked_deals_in_snapshot_quarter)                                                    AS first_order_booked_deals_in_snapshot_quarter,
    SUM(first_order_saos_generated_in_snapshot_quarter)                                                  AS first_order_saos_generated_in_snapshot_quarter, 
    SUM(booked_ps_value_in_snapshot_quarter)                                                             AS booked_ps_value_in_snapshot_quarter
  FROM actuals
  {{ dbt_utils.group_by(n=7) }}
),

quarterly_actuals AS (

  SELECT
    live_actuals.close_fiscal_quarter_name,
    live_actuals.close_fiscal_quarter_date,
    live_actuals.dim_crm_current_account_set_hierarchy_sk,
    live_actuals.dim_sales_qualified_source_id,
    live_actuals.dim_order_type_id,
    SUM(live_actuals.booked_net_arr)              AS total_booked_net_arr,
    SUM(live_actuals.created_arr)                 AS total_created_arr,
    SUM(live_actuals.booked_ps_value)             AS total_booked_ps_value,
    SUM(live_actuals.first_order_booked_deals)    AS total_first_order_booked_deals,
    SUM(live_actuals.first_order_saos_generated)  AS total_first_order_saos_generated 
  FROM live_actuals
  {{ dbt_utils.group_by(n=5) }}


),

combined_data AS (

  SELECT
    dim_crm_current_account_set_hierarchy_sk,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    fiscal_quarter_name,
    fiscal_quarter_date
  FROM total_targets

  UNION

  SELECT
    dim_crm_current_account_set_hierarchy_sk,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    close_fiscal_quarter_name,
    close_fiscal_quarter_date
  FROM quarterly_actuals

  UNION

  SELECT
    dim_crm_current_account_set_hierarchy_sk,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    snapshot_fiscal_quarter_name,
    snapshot_fiscal_quarter_date
  FROM daily_actuals

),

base AS (

  /*
    Cross join all dimensions (hierarchy, qualified source, order type) and
    the dates to create a comprehensive set of all possible combinations of these dimensions and dates.
    This exhaustive combination is essential for scenarios where we need to account for all possible configurations in our analysis,
    ensuring that no combination is overlooked.

    When we eventually join this set of combinations with the quarterly actuals,
    it ensures that even the newly introduced dimensions are accounted for.
  */

  SELECT
    combined_data.dim_crm_current_account_set_hierarchy_sk,
    combined_data.dim_sales_qualified_source_id,
    combined_data.dim_order_type_id,
    prep_date.date_id,
    prep_date.date_actual,
    prep_date.last_day_of_fiscal_quarter,
    prep_date.first_day_of_fiscal_year,
    prep_date.current_first_day_of_fiscal_year,
    combined_data.fiscal_quarter_date,
    combined_data.fiscal_quarter_name,
    max_snapshot_date.max_snapshot_date_actuals
  FROM combined_data
  INNER JOIN prep_date
    ON combined_data.fiscal_quarter_name = prep_date.fiscal_quarter_name_fy
  CROSS JOIN max_snapshot_date
  WHERE prep_date.first_day_of_fiscal_year >=  DATEADD('year', -3, prep_date.current_first_day_of_fiscal_year)

),

final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['base.date_id', 'base.dim_crm_current_account_set_hierarchy_sk', 'base.dim_order_type_id','base.dim_sales_qualified_source_id']) }} AS targets_actuals_daily_snapshot_pk,
    base.date_id,
    base.date_actual,
    base.last_day_of_fiscal_quarter,
    base.fiscal_quarter_name,
    base.fiscal_quarter_date,
    base.dim_crm_current_account_set_hierarchy_sk,
    base.dim_order_type_id,
    base.dim_sales_qualified_source_id,
    base.max_snapshot_date_actuals,
    SUM(total_targets.pipeline_created_total_quarter_target)                                             AS pipeline_created_total_quarter_target,
    SUM(total_targets.net_arr_total_quarter_target)                                                      AS net_arr_total_quarter_target,
    SUM(total_targets.ps_value_total_quarter_target)                                                     AS ps_value_total_quarter_target,
    SUM(total_targets.new_logos_total_quarter_target)                                                    AS new_logos_total_quarter_target,
    SUM(total_targets.saos_total_quarter_target)                                                         AS saos_total_quarter_target, 
    CASE WHEN base.date_actual = base.last_day_of_fiscal_quarter
        THEN SUM(quarterly_actuals.total_booked_net_arr)
      ELSE SUM(daily_actuals.booked_net_arr_in_snapshot_quarter)
    END                                                                                                  AS booked_net_arr,
    SUM(daily_actuals.open_1plus_net_arr_in_snapshot_quarter)                                            AS open_1plus_net_arr,
    SUM(daily_actuals.open_3plus_net_arr_in_snapshot_quarter)                                            AS open_3plus_net_arr,
    SUM(daily_actuals.open_4plus_net_arr_in_snapshot_quarter)                                            AS open_4plus_net_arr,
    CASE WHEN base.date_actual = base.last_day_of_fiscal_quarter
        THEN SUM(quarterly_actuals.total_created_arr)
      ELSE SUM(daily_actuals.created_arr_in_snapshot_quarter)
    END                                                                                                  AS created_arr,
    CASE WHEN base.date_actual = base.last_day_of_fiscal_quarter
        THEN SUM(quarterly_actuals.total_first_order_booked_deals)
      ELSE SUM(daily_actuals.first_order_booked_deals_in_snapshot_quarter)
    END                                                                                                  AS first_order_booked_deals,
    CASE WHEN base.date_actual = base.last_day_of_fiscal_quarter
        THEN SUM(quarterly_actuals.total_first_order_saos_generated)
      ELSE SUM(daily_actuals.first_order_saos_generated_in_snapshot_quarter)
    END                                                                                                  AS first_order_saos_generated,
    CASE WHEN base.date_actual = base.last_day_of_fiscal_quarter
        THEN SUM(quarterly_actuals.total_booked_ps_value)
      ELSE SUM(daily_actuals.booked_ps_value_in_snapshot_quarter)
    END                                                                                                  AS booked_ps_value,
    SUM(quarterly_actuals.total_booked_net_arr)                                                          AS total_booked_net_arr
  FROM base
  LEFT JOIN total_targets
    ON base.fiscal_quarter_name = total_targets.fiscal_quarter_name
      AND base.dim_sales_qualified_source_id = total_targets.dim_sales_qualified_source_id
      AND base.dim_crm_current_account_set_hierarchy_sk = total_targets.dim_crm_current_account_set_hierarchy_sk
      AND base.dim_order_type_id = total_targets.dim_order_type_id
  LEFT JOIN daily_actuals
    ON base.date_id = daily_actuals.snapshot_id
      AND base.dim_sales_qualified_source_id = daily_actuals.dim_sales_qualified_source_id
      AND base.dim_crm_current_account_set_hierarchy_sk = daily_actuals.dim_crm_current_account_set_hierarchy_sk
      AND base.dim_order_type_id = daily_actuals.dim_order_type_id
  LEFT JOIN quarterly_actuals
    ON base.fiscal_quarter_name = quarterly_actuals.close_fiscal_quarter_name
      AND base.dim_sales_qualified_source_id = quarterly_actuals.dim_sales_qualified_source_id
      AND base.dim_crm_current_account_set_hierarchy_sk = quarterly_actuals.dim_crm_current_account_set_hierarchy_sk
      AND base.dim_order_type_id = quarterly_actuals.dim_order_type_id
  {{ dbt_utils.group_by(n=10) }}

)

SELECT *
FROM final

