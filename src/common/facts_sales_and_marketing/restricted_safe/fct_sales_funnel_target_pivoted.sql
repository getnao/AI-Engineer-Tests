{{ simple_cte([
    ('dim_date','dim_date'),
    ('targets', 'fct_sales_funnel_target')
]) }},

-- fct_sales_funnel_target only has monthly grain so in this CTE we join on dim_date to obtain a daily grain
targets_with_dates AS (
    SELECT
      -- Date info
      dim_date.date_actual                                                          AS target_date,
      dim_date.date_id                                                              AS target_date_id,
      dim_date.first_day_of_month                                                   AS target_month_date,
      targets.target_month_id,
      dim_date.first_day_of_month,   
      dim_date.fiscal_quarter_name_fy,
      dim_date.last_day_of_fiscal_quarter,
      dim_date.fiscal_year,
      dim_date.last_day_of_fiscal_year,
      -- Add indicators for first day of month/quarter/year
      IFF(dim_date.day_of_month = 1, TRUE, FALSE)                                   AS is_first_day_of_month,
      IFF(dim_date.first_day_of_fiscal_quarter = dim_date.date_actual, TRUE, FALSE) AS is_first_day_of_quarter,
      IFF(dim_date.first_day_of_fiscal_year = dim_date.date_actual, TRUE, FALSE)    AS is_first_day_of_year,

      -- Dimensional information 
      targets.dim_crm_user_hierarchy_sk,
      targets.dim_sales_qualified_source_id,
      targets.dim_order_type_id,

      -- Surrogate key to simplify joining and aggregation later as these 3 dimensions get used together to set up targets
      {{ dbt_utils.generate_surrogate_key(['dim_crm_user_hierarchy_sk', 'dim_sales_qualified_source_id', 'dim_order_type_id']) }} AS target_category_sk,

      -- Target information
      targets.kpi_name,
      (targets.allocated_target / dim_date.days_in_month_count) AS daily_allocated_target -- in order to convert the monthly target into a daily one we divide by the number of days in that target month
    FROM targets   
    LEFT JOIN dim_date
      ON targets.first_day_of_month = dim_date.first_day_of_month
),

targets_daily AS (
  SELECT 
    -- Dates
    target_date,
    target_date_id,
    target_month_date,
    target_month_id,
    first_day_of_month, 
    fiscal_quarter_name_fy,
    last_day_of_fiscal_quarter,
    fiscal_year,
    last_day_of_fiscal_year,
    is_first_day_of_month,
    is_first_day_of_quarter,
    is_first_day_of_year,

    -- Dimensions
    dim_crm_user_hierarchy_sk,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    target_category_sk,

    -- Aggregations
    SUM(IFF(kpi_name = 'Deals', daily_allocated_target , 0))                    AS deals_daily_allocated_target,
    SUM(IFF(kpi_name = 'New Logos', daily_allocated_target, 0))                 AS new_logos_daily_allocated_target,
    SUM(IFF(kpi_name = 'Stage 1 Opportunities', daily_allocated_target, 0))     AS saos_daily_allocated_target,
    SUM(IFF(kpi_name = 'Net ARR', daily_allocated_target, 0))                   AS net_arr_daily_allocated_target,
    SUM(IFF(kpi_name = 'ATR', daily_allocated_target, 0))                       AS atr_daily_allocated_target,
    SUM(IFF(kpi_name = 'Partner Net ARR', daily_allocated_target, 0))           AS partner_net_arr_daily_allocated_target,
    SUM(IFF(kpi_name = 'PS Value', daily_allocated_target, 0))                  AS ps_value_daily_allocated_target,
    SUM(IFF(kpi_name = 'PS Value Pipeline Created', daily_allocated_target, 0)) AS ps_value_pipeline_created_daily_allocated_target,
    SUM(IFF(kpi_name = 'Net ARR Pipeline Created', daily_allocated_target, 0))  AS net_arr_pipeline_created_daily_allocated_target,
    SUM(IFF(kpi_name = 'Churn/Contraction Amount', daily_allocated_target, 0))  AS churn_contraction_amount_daily_allocated_target
  FROM targets_with_dates
  {{ dbt_utils.group_by(n=16) }}
),

-- Separate CTE required for monthly, quarterly and yearly aggregations so as to avoid overcounting
monthly_categories_distinct AS (
  SELECT DISTINCT
    targets.target_month_id,
    dim_date.fiscal_quarter_name_fy,
    dim_date.fiscal_year,
    {{ dbt_utils.generate_surrogate_key(['dim_crm_user_hierarchy_sk', 'dim_sales_qualified_source_id', 'dim_order_type_id']) }} AS target_category_sk,
    targets.kpi_name,
    targets.allocated_target,
    (targets.allocated_target / dim_date.days_in_month_count) AS daily_allocated_target
  FROM targets
  LEFT JOIN dim_date
  ON targets.target_month_id = dim_date.date_id 
),

-- In this CTE we only need to use MAX rather than SUM because monthly_categories_distinct is already at a date grain 
targets_monthly AS (
  SELECT 
    target_month_id,
    fiscal_quarter_name_fy,
    fiscal_year,
    target_category_sk,
    MAX(IFF(kpi_name = 'Deals', allocated_target, 0))                       AS deals_monthly_allocated_target,
    MAX(IFF(kpi_name = 'New Logos', allocated_target, 0))                   AS new_logos_monthly_allocated_target,
    MAX(IFF(kpi_name = 'Stage 1 Opportunities', allocated_target, 0))       AS saos_monthly_allocated_target,
    MAX(IFF(kpi_name = 'Net ARR', allocated_target, 0))                     AS net_arr_monthly_allocated_target,
    MAX(IFF(kpi_name = 'ATR', allocated_target, 0))                         AS atr_monthly_allocated_target,
    MAX(IFF(kpi_name = 'Partner Net ARR', allocated_target, 0))             AS partner_net_arr_monthly_allocated_target,
    MAX(IFF(kpi_name = 'PS Value', allocated_target, 0))                    AS ps_value_monthly_allocated_target,
    MAX(IFF(kpi_name = 'PS Value Pipeline Created', allocated_target, 0))   AS ps_value_pipeline_created_monthly_allocated_target,
    MAX(IFF(kpi_name = 'Net ARR Pipeline Created', allocated_target, 0))    AS net_arr_pipeline_created_monthly_allocated_target,
    MAX(IFF(kpi_name = 'Churn/Contraction Amount', allocated_target, 0))    AS churn_contraction_amount_monthly_allocated_target
  FROM monthly_categories_distinct
  {{ dbt_utils.group_by(n=4) }}
),

-- Quarterly targets
targets_quarterly AS (
  SELECT 
    fiscal_quarter_name_fy,
    fiscal_year,
    target_category_sk,
    SUM(deals_monthly_allocated_target)                     AS deals_quarterly_allocated_target,
    SUM(new_logos_monthly_allocated_target)                 AS new_logos_quarterly_allocated_target,
    SUM(saos_monthly_allocated_target)                      AS saos_quarterly_allocated_target,
    SUM(net_arr_monthly_allocated_target)                   AS net_arr_quarterly_allocated_target,
    SUM(atr_monthly_allocated_target)                       AS atr_quarterly_allocated_target,
    SUM(partner_net_arr_monthly_allocated_target)           AS partner_net_arr_quarterly_allocated_target,
    SUM(ps_value_monthly_allocated_target)                  AS ps_value_quarterly_allocated_target,
    SUM(ps_value_pipeline_created_monthly_allocated_target) AS ps_value_pipeline_created_quarterly_allocated_target,
    SUM(net_arr_pipeline_created_monthly_allocated_target)  AS net_arr_pipeline_created_quarterly_allocated_target,
    SUM(churn_contraction_amount_monthly_allocated_target)  AS churn_contraction_amount_quarterly_allocated_target
  FROM targets_monthly
  {{ dbt_utils.group_by(n=3) }}
),

-- Yearly targets
targets_yearly AS (
  SELECT 
    fiscal_year,
    target_category_sk,
    SUM(deals_quarterly_allocated_target)                     AS deals_yearly_allocated_target,
    SUM(new_logos_quarterly_allocated_target)                 AS new_logos_yearly_allocated_target,
    SUM(saos_quarterly_allocated_target)                      AS saos_yearly_allocated_target,
    SUM(net_arr_quarterly_allocated_target)                   AS net_arr_yearly_allocated_target,
    SUM(atr_quarterly_allocated_target)                       AS atr_yearly_allocated_target,
    SUM(partner_net_arr_quarterly_allocated_target)           AS partner_net_arr_yearly_allocated_target,
    SUM(ps_value_quarterly_allocated_target)                  AS ps_value_yearly_allocated_target,
    SUM(net_arr_pipeline_created_quarterly_allocated_target)  AS ps_value_pipeline_created_yearly_allocated_target,
    SUM(net_arr_pipeline_created_quarterly_allocated_target)  AS net_arr_pipeline_created_yearly_allocated_target,
    SUM(churn_contraction_amount_quarterly_allocated_target)  AS churn_contraction_amount_yearly_allocated_target
  FROM targets_quarterly
  {{ dbt_utils.group_by(n=2) }}
),

targets_to_date AS (

  SELECT DISTINCT -- duplicates appear owing to there being a row for each KPI in targets_with_dates so we need to obtain the distinct values
    target_date_id,
    target_category_sk,
    
    -- Targets to date: calculating how far we'd need to be by this date in order to meet the target for a given period
    SUM(IFF(kpi_name = 'Deals', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, target_month_date ORDER BY target_date)      AS deals_month_to_date_allocated_target, 
    SUM(IFF(kpi_name = 'Deals', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_quarter_name_fy ORDER BY target_date) AS deals_quarter_to_date_allocated_target,
    SUM(IFF(kpi_name = 'Deals', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_year ORDER BY target_date)            AS deals_year_to_date_allocated_target,

    -- New Logos to date targets
    SUM(IFF(kpi_name = 'New Logos', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, target_month_date ORDER BY target_date)      AS new_logos_month_to_date_allocated_target, 
    SUM(IFF(kpi_name = 'New Logos', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_quarter_name_fy ORDER BY target_date) AS new_logos_quarter_to_date_allocated_target,
    SUM(IFF(kpi_name = 'New Logos', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_year ORDER BY target_date)            AS new_logos_year_to_date_allocated_target,

    -- Stage 1 Opportunities to date targets
    SUM(IFF(kpi_name = 'Stage 1 Opportunities', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, target_month_date ORDER BY target_date)      AS saos_month_to_date_allocated_target, 
    SUM(IFF(kpi_name = 'Stage 1 Opportunities', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_quarter_name_fy ORDER BY target_date) AS saos_quarter_to_date_allocated_target,
    SUM(IFF(kpi_name = 'Stage 1 Opportunities', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_year ORDER BY target_date)            AS saos_year_to_date_allocated_target,

    -- Net ARR to date targets
    SUM(IFF(kpi_name = 'Net ARR', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, target_month_date ORDER BY target_date)      AS net_arr_month_to_date_allocated_target, 
    SUM(IFF(kpi_name = 'Net ARR', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_quarter_name_fy ORDER BY target_date) AS net_arr_quarter_to_date_allocated_target,
    SUM(IFF(kpi_name = 'Net ARR', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_year ORDER BY target_date)            AS net_arr_year_to_date_allocated_target,

    -- ATR to date targets
    SUM(IFF(kpi_name = 'ATR', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, target_month_date ORDER BY target_date)      AS atr_month_to_date_allocated_target, 
    SUM(IFF(kpi_name = 'ATR', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_quarter_name_fy ORDER BY target_date) AS atr_quarter_to_date_allocated_target,
    SUM(IFF(kpi_name = 'ATR', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_year ORDER BY target_date)            AS atr_year_to_date_allocated_target,

    -- Partner Net ARR to date targets
    SUM(IFF(kpi_name = 'Partner Net ARR', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, target_month_date ORDER BY target_date)      AS partner_net_arr_month_to_date_allocated_target, 
    SUM(IFF(kpi_name = 'Partner Net ARR', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_quarter_name_fy ORDER BY target_date) AS partner_net_arr_quarter_to_date_allocated_target,
    SUM(IFF(kpi_name = 'Partner Net ARR', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_year ORDER BY target_date)            AS partner_net_arr_year_to_date_allocated_target,

    -- PS Value to date targets
    SUM(IFF(kpi_name = 'PS Value', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, target_month_date ORDER BY target_date)      AS ps_value_month_to_date_allocated_target, 
    SUM(IFF(kpi_name = 'PS Value', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_quarter_name_fy ORDER BY target_date) AS ps_value_quarter_to_date_allocated_target,
    SUM(IFF(kpi_name = 'PS Value', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_year ORDER BY target_date)            AS ps_value_year_to_date_allocated_target,

    -- PS Value Pipeline Created to date targets
    SUM(IFF(kpi_name = 'PS Value Pipeline Created', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, target_month_date ORDER BY target_date)      AS ps_value_pipeline_created_month_to_date_allocated_target, 
    SUM(IFF(kpi_name = 'PS Value Pipeline Created', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_quarter_name_fy ORDER BY target_date) AS ps_value_pipeline_created_quarter_to_date_allocated_target,
    SUM(IFF(kpi_name = 'PS Value Pipeline Created', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_year ORDER BY target_date)            AS ps_value_pipeline_created_year_to_date_allocated_target,

    -- Net ARR Pipeline Created to date targets
    SUM(IFF(kpi_name = 'Net ARR Pipeline Created', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, target_month_date ORDER BY target_date)      AS net_arr_pipeline_created_month_to_date_allocated_target, 
    SUM(IFF(kpi_name = 'Net ARR Pipeline Created', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_quarter_name_fy ORDER BY target_date) AS net_arr_pipeline_created_quarter_to_date_allocated_target,
    SUM(IFF(kpi_name = 'Net ARR Pipeline Created', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_year ORDER BY target_date)            AS net_arr_pipeline_created_year_to_date_allocated_target,

    -- Churn/Contraction Amount to date targets
    SUM(IFF(kpi_name = 'Churn/Contraction Amount', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, target_month_date ORDER BY target_date)      AS churn_contraction_amount_month_to_date_allocated_target, 
    SUM(IFF(kpi_name = 'Churn/Contraction Amount', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_quarter_name_fy ORDER BY target_date) AS churn_contraction_amount_quarter_to_date_allocated_target,
    SUM(IFF(kpi_name = 'Churn/Contraction Amount', daily_allocated_target, 0)) 
      OVER(PARTITION BY target_category_sk, fiscal_year ORDER BY target_date)            AS churn_contraction_amount_year_to_date_allocated_target
    FROM targets_with_dates
),

final AS (

  SELECT
    -- Key
    {{ dbt_utils.generate_surrogate_key(['targets_daily.target_date','targets_daily.dim_crm_user_hierarchy_sk', 'targets_daily.dim_sales_qualified_source_id', 'targets_daily.dim_order_type_id']) }} AS target_date_category_sk, -- unique key for this fact table

    -- Dates 
    targets_daily.target_date,
    targets_daily.target_date_id,
    targets_daily.target_month_date,
    targets_daily.fiscal_quarter_name_fy,
    targets_daily.fiscal_year,

    -- IDs
    targets_daily.dim_crm_user_hierarchy_sk,
    targets_daily.dim_sales_qualified_source_id,
    targets_daily.dim_order_type_id,

    -- Deals 
    targets_daily.deals_daily_allocated_target,
    -- Only show monthly targets on the first day of the month
    IFF(targets_daily.is_first_day_of_month, 
        COALESCE(targets_monthly.deals_monthly_allocated_target, 0),
        0) AS deals_monthly_allocated_target,
    -- Only show quarterly targets on the first day of the quarter
    IFF(targets_daily.is_first_day_of_quarter,
        COALESCE(targets_quarterly.deals_quarterly_allocated_target, 0),
        0) AS deals_quarterly_allocated_target,
    -- Only show yearly targets on the first day of the year
    IFF(targets_daily.is_first_day_of_year,
        COALESCE(targets_yearly.deals_yearly_allocated_target, 0),
        0) AS deals_yearly_allocated_target,
    targets_to_date.deals_month_to_date_allocated_target,
    targets_to_date.deals_quarter_to_date_allocated_target,
    targets_to_date.deals_year_to_date_allocated_target,
    
    -- New Logos 
    targets_daily.new_logos_daily_allocated_target,
    IFF(targets_daily.is_first_day_of_month, 
        NVL(targets_monthly.new_logos_monthly_allocated_target, 0), 0)                      AS new_logos_monthly_allocated_target, 
    IFF(targets_daily.is_first_day_of_quarter,                
        NVL(targets_quarterly.new_logos_quarterly_allocated_target, 0), 0)                  AS new_logos_quarterly_allocated_target,
    IFF(targets_daily.is_first_day_of_year,               
        NVL(targets_yearly.new_logos_yearly_allocated_target, 0), 0)                        AS new_logos_yearly_allocated_target, 
    targets_to_date.new_logos_month_to_date_allocated_target,
    targets_to_date.new_logos_quarter_to_date_allocated_target,
    targets_to_date.new_logos_year_to_date_allocated_target, 
    
    -- Stage 1 Opportunities (SAOs) 
    targets_daily.saos_daily_allocated_target,
    IFF(targets_daily.is_first_day_of_month, 
        NVL(targets_monthly.saos_monthly_allocated_target, 0), 0)                           AS saos_monthly_allocated_target,
    IFF(targets_daily.is_first_day_of_quarter,
        NVL(targets_quarterly.saos_quarterly_allocated_target, 0), 0)                       AS saos_quarterly_allocated_target,
    IFF(targets_daily.is_first_day_of_year,
        NVL(targets_yearly.saos_yearly_allocated_target, 0), 0)                             AS saos_yearly_allocated_target,
    targets_to_date.saos_month_to_date_allocated_target,
    targets_to_date.saos_quarter_to_date_allocated_target,
    targets_to_date.saos_year_to_date_allocated_target,
    
    -- Net ARR 
    targets_daily.net_arr_daily_allocated_target,
    IFF(targets_daily.is_first_day_of_month, 
        NVL(targets_monthly.net_arr_monthly_allocated_target, 0), 0)                        AS net_arr_monthly_allocated_target,
    IFF(targets_daily.is_first_day_of_quarter,                
        NVL(targets_quarterly.net_arr_quarterly_allocated_target, 0), 0)                    AS net_arr_quarterly_allocated_target,
    IFF(targets_daily.is_first_day_of_year,               
        NVL(targets_yearly.net_arr_yearly_allocated_target, 0), 0)                          AS net_arr_yearly_allocated_target,
    targets_to_date.net_arr_month_to_date_allocated_target,
    targets_to_date.net_arr_quarter_to_date_allocated_target,
    targets_to_date.net_arr_year_to_date_allocated_target,
    
    -- ATR 
    targets_daily.atr_daily_allocated_target,
    IFF(targets_daily.is_first_day_of_month, 
        NVL(targets_monthly.atr_monthly_allocated_target, 0), 0)                            AS atr_monthly_allocated_target,
    IFF(targets_daily.is_first_day_of_quarter,                
        NVL(targets_quarterly.atr_quarterly_allocated_target, 0), 0)                        AS atr_quarterly_allocated_target,
    IFF(targets_daily.is_first_day_of_year,               
        NVL(targets_yearly.atr_yearly_allocated_target, 0), 0)                              AS atr_yearly_allocated_target,
    targets_to_date.atr_month_to_date_allocated_target,
    targets_to_date.atr_quarter_to_date_allocated_target,
    targets_to_date.atr_year_to_date_allocated_target,
    
    -- Partner Net ARR 
    targets_daily.partner_net_arr_daily_allocated_target,
    IFF(targets_daily.is_first_day_of_month, 
        NVL(targets_monthly.partner_net_arr_monthly_allocated_target, 0), 0)                AS partner_net_arr_monthly_allocated_target,
    IFF(targets_daily.is_first_day_of_quarter,          
        NVL(targets_quarterly.partner_net_arr_quarterly_allocated_target, 0), 0)            AS partner_net_arr_quarterly_allocated_target,
    IFF(targets_daily.is_first_day_of_year,         
        NVL(targets_yearly.partner_net_arr_yearly_allocated_target, 0), 0)                  AS partner_net_arr_yearly_allocated_target,
    targets_to_date.partner_net_arr_month_to_date_allocated_target,
    targets_to_date.partner_net_arr_quarter_to_date_allocated_target,
    targets_to_date.partner_net_arr_year_to_date_allocated_target,
    
    -- PS Value 
    targets_daily.ps_value_daily_allocated_target,
    IFF(targets_daily.is_first_day_of_month, 
        NVL(targets_monthly.ps_value_monthly_allocated_target, 0), 0)                       AS ps_value_monthly_allocated_target,
    IFF(targets_daily.is_first_day_of_quarter,          
        NVL(targets_quarterly.ps_value_quarterly_allocated_target, 0), 0)                   AS ps_value_quarterly_allocated_target,
    IFF(targets_daily.is_first_day_of_year,         
        NVL(targets_yearly.ps_value_yearly_allocated_target, 0), 0)                         AS ps_value_yearly_allocated_target,
    targets_to_date.ps_value_month_to_date_allocated_target,
    targets_to_date.ps_value_quarter_to_date_allocated_target,
    targets_to_date.ps_value_year_to_date_allocated_target,
    
    -- PS Value Pipeline Created 
    targets_daily.ps_value_pipeline_created_daily_allocated_target,
    IFF(targets_daily.is_first_day_of_month, 
        NVL(targets_monthly.ps_value_pipeline_created_monthly_allocated_target, 0), 0)      AS ps_value_pipeline_created_monthly_allocated_target,
    IFF(targets_daily.is_first_day_of_quarter,
        NVL(targets_quarterly.ps_value_pipeline_created_quarterly_allocated_target, 0), 0)  AS ps_value_pipeline_created_quarterly_allocated_target,
    IFF(targets_daily.is_first_day_of_year,
        NVL(targets_yearly.ps_value_pipeline_created_yearly_allocated_target, 0), 0)        AS ps_value_pipeline_created_yearly_allocated_target,
    targets_to_date.ps_value_pipeline_created_month_to_date_allocated_target,
    targets_to_date.ps_value_pipeline_created_quarter_to_date_allocated_target,
    targets_to_date.ps_value_pipeline_created_year_to_date_allocated_target,
    
    -- Net ARR Pipeline Created 
    targets_daily.net_arr_pipeline_created_daily_allocated_target,
    IFF(targets_daily.is_first_day_of_month, 
        NVL(targets_monthly.net_arr_pipeline_created_monthly_allocated_target, 0), 0)       AS net_arr_pipeline_created_monthly_allocated_target,
    IFF(targets_daily.is_first_day_of_quarter,
        NVL(targets_quarterly.net_arr_pipeline_created_quarterly_allocated_target, 0), 0)   AS net_arr_pipeline_created_quarterly_allocated_target,
    IFF(targets_daily.is_first_day_of_year,
        NVL(targets_yearly.net_arr_pipeline_created_yearly_allocated_target, 0), 0)         AS net_arr_pipeline_created_yearly_allocated_target,
    targets_to_date.net_arr_pipeline_created_month_to_date_allocated_target,
    targets_to_date.net_arr_pipeline_created_quarter_to_date_allocated_target,
    targets_to_date.net_arr_pipeline_created_year_to_date_allocated_target,
    
    -- Churn/Contraction Amount 
    targets_daily.churn_contraction_amount_daily_allocated_target,
    IFF(targets_daily.is_first_day_of_month, 
        NVL(targets_monthly.churn_contraction_amount_monthly_allocated_target, 0), 0)       AS churn_contraction_amount_monthly_allocated_target,
    IFF(targets_daily.is_first_day_of_quarter,
        NVL(targets_quarterly.churn_contraction_amount_quarterly_allocated_target, 0), 0)   AS churn_contraction_amount_quarterly_allocated_target,
    IFF(targets_daily.is_first_day_of_year,
        NVL(targets_yearly.churn_contraction_amount_yearly_allocated_target, 0), 0)         AS churn_contraction_amount_yearly_allocated_target,
    targets_to_date.churn_contraction_amount_month_to_date_allocated_target,
    targets_to_date.churn_contraction_amount_quarter_to_date_allocated_target,
    targets_to_date.churn_contraction_amount_year_to_date_allocated_target
  FROM targets_daily
  LEFT JOIN targets_monthly
    ON targets_daily.target_month_id = targets_monthly.target_month_id
    AND targets_daily.target_category_sk = targets_monthly.target_category_sk
  LEFT JOIN targets_quarterly
    ON targets_daily.fiscal_quarter_name_fy = targets_quarterly.fiscal_quarter_name_fy
    AND targets_daily.target_category_sk = targets_quarterly.target_category_sk
  LEFT JOIN targets_yearly
    ON targets_daily.fiscal_year = targets_yearly.fiscal_year
    AND targets_daily.target_category_sk = targets_yearly.target_category_sk 
  LEFT JOIN targets_to_date
    ON targets_daily.target_date_id = targets_to_date.target_date_id
    AND targets_daily.target_category_sk = targets_to_date.target_category_sk

)
SELECT * 
FROM final