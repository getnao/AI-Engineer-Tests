{{ simple_cte([
    ('driveload_financial_metrics_program_phase_1_source','driveload_financial_metrics_program_phase_1_source'),
    ('dim_date','dim_date'),
    ('mart_arr_snapshot_model','mart_arr_snapshot_model'),
    ('mart_arr_snapshot_bottom_up','mart_arr_snapshot_bottom_up'),
    ('mart_arr_current', 'mart_arr')
]) }},

dim_date_actual AS (

  -- This CTE controls which months will be added from `mart_arr_current` to this table. It includes logic to:
  -- 1. Use 8th calendar day before 2024-03-01
  -- 2. Use 5th calendar day from 2024-03-01 through 2025-04-30
  -- 3. Use 4th calendar day from 2025-05-01 onwards
  -- If the last date in the snapshot table is below the snapshot day threshold, we have to add two dates:
  -- the max month of the snapshot and the month before it
  -- Example: if last date snapshot = 2023-08-04, then we need to add 2023-07-01 (as we are not yet on 2023-08-05) and also 2023-08-01 (current_month) to the table
  SELECT
    first_day_of_month,
    CASE 
      WHEN first_day_of_month < '2024-03-01' THEN snapshot_date_fpa
      WHEN first_day_of_month < '2025-05-01' THEN snapshot_date_fpa_fifth
      ELSE snapshot_date_fpa_fourth
    END                                                      AS snapshot_date_fpa,
    date_actual,
    (SELECT MAX(snapshot_date) FROM mart_arr_snapshot_model) AS max_snapshot_date
  FROM dim_date
  WHERE CASE
      WHEN DAY(max_snapshot_date) <= 3
        THEN date_actual = max_snapshot_date
          OR date_actual = DATEADD('month', -1, max_snapshot_date)
      ELSE date_actual = max_snapshot_date
    END

),

snapshot_dates AS (
  -- Use the following calendar days to snapshot ARR, Licensed Users, and Customer Count Metrics:
  -- 1. 8th calendar day before 2024-03-01
  -- 2. 5th calendar day from 2024-03-01 through 2025-04-30
  -- 3. 4th calendar day from 2025-05-01 onwards
  SELECT DISTINCT
    first_day_of_month,
    CASE 
      WHEN first_day_of_month < '2024-03-01' THEN snapshot_date_fpa
      WHEN first_day_of_month < '2025-05-01' THEN snapshot_date_fpa_fifth
      ELSE snapshot_date_fpa_fourth
    END AS snapshot_date_fpa
  FROM dim_date
  ORDER BY 1 DESC

),

mart_arr_snapshot_model_combined AS (

  SELECT
    TRUE                                                                                AS is_arr_month_finalized,
    snapshot_date,
    arr_month,
    fiscal_quarter_name_fy,
    fiscal_year,
    subscription_start_month,
    subscription_end_month,
    dim_billing_account_id,
    sold_to_country,
    billing_account_name,
    billing_account_number,
    dim_crm_account_id,
    dim_parent_crm_account_id,
    parent_crm_account_name,
    parent_crm_account_billing_country,
    -- Coalescing these two values since sales segment has been changed in SFDC but finance reporting needs to keep using the legacy value
    -- Background issue: https://gitlab.com/gitlab-data/analytics/-/issues/20395
    COALESCE(parent_crm_account_sales_segment_legacy, parent_crm_account_sales_segment) AS parent_crm_account_sales_segment,
    parent_crm_account_industry,
    parent_crm_account_geo,
    parent_crm_account_owner_team,
    parent_crm_account_sales_territory,
    dim_subscription_id,
    subscription_name,
    subscription_status,
    subscription_sales_type,
    product_tier_name,
    product_rate_plan_name,
    product_rate_plan_charge_name,
    product_deployment_type,
    product_delivery_type,
    product_ranking,
    service_type,
    unit_of_measure,
    mrr,
    arr,
    quantity,
    is_arpu,
    dim_charge_id,
    is_licensed_user,
    parent_crm_account_employee_count_band,
    is_jihu_account
  FROM mart_arr_snapshot_model

  UNION ALL

  SELECT
    FALSE                                                    AS is_arr_month_finalized,
    dim_date_actual.snapshot_date_fpa                        AS snapshot_date,
    mart_arr_current.arr_month,
    COALESCE(
      mart_arr_current.fiscal_quarter_name_fy,
      CASE WHEN dim_date.current_first_day_of_month = dim_date.first_day_of_month
          THEN dim_date.fiscal_quarter_name_fy
      END
    )                                                        AS fiscal_quarter_name_fy,
    COALESCE(
      mart_arr_current.fiscal_year,
      CASE WHEN dim_date.current_first_day_of_month = dim_date.first_day_of_month
          THEN dim_date.fiscal_year
      END
    )                                                        AS fiscal_year,
    mart_arr_current.subscription_start_month,
    mart_arr_current.subscription_end_month,
    mart_arr_current.dim_billing_account_id,
    mart_arr_current.sold_to_country,
    mart_arr_current.billing_account_name,
    mart_arr_current.billing_account_number,
    mart_arr_current.dim_crm_account_id,
    mart_arr_current.dim_parent_crm_account_id,
    mart_arr_current.parent_crm_account_name,
    NULL                                                     AS parent_crm_account_billing_country,
    -- Sales segment has been changed in SFDC but finance reporting needs to keep using the legacy value
    -- Background issue: https://gitlab.com/gitlab-data/analytics/-/issues/20395
    mart_arr_current.parent_crm_account_sales_segment_legacy AS parent_crm_account_sales_segment,
    mart_arr_current.parent_crm_account_industry,
    mart_arr_current.parent_crm_account_geo,
    NULL                                                     AS parent_crm_account_owner_team,
    NULL                                                     AS parent_crm_account_sales_territory,
    mart_arr_current.dim_subscription_id,
    mart_arr_current.subscription_name,
    mart_arr_current.subscription_status,
    mart_arr_current.subscription_sales_type,
    mart_arr_current.product_tier_name,
    mart_arr_current.product_rate_plan_name,
    mart_arr_current.product_rate_plan_charge_name,
    mart_arr_current.product_deployment_type,
    mart_arr_current.product_delivery_type,
    mart_arr_current.product_ranking,
    mart_arr_current.service_type,
    mart_arr_current.unit_of_measure,
    mart_arr_current.mrr,
    mart_arr_current.arr,
    mart_arr_current.quantity,
    mart_arr_current.is_arpu,
    mart_arr_current.dim_charge_id,
    mart_arr_current.is_licensed_user,
    NULL                                                     AS parent_crm_account_employee_count_band,
    mart_arr_current.is_jihu_account
  FROM mart_arr_current
  INNER JOIN dim_date_actual
    ON mart_arr_current.arr_month = dim_date_actual.first_day_of_month
  INNER JOIN dim_date
    ON mart_arr_current.arr_month = dim_date.date_actual

),

phase_one AS (

  SELECT
    TRUE                                                                     AS is_arr_month_finalized,
    driveload_financial_metrics_program_phase_1_source.arr_month,
    driveload_financial_metrics_program_phase_1_source.fiscal_quarter_name_fy,
    driveload_financial_metrics_program_phase_1_source.fiscal_year,
    driveload_financial_metrics_program_phase_1_source.subscription_start_month,
    driveload_financial_metrics_program_phase_1_source.subscription_end_month,
    driveload_financial_metrics_program_phase_1_source.zuora_account_id      AS dim_billing_account_name,
    driveload_financial_metrics_program_phase_1_source.zuora_sold_to_country AS sold_to_country,
    driveload_financial_metrics_program_phase_1_source.zuora_account_name    AS billing_account_name,
    driveload_financial_metrics_program_phase_1_source.zuora_account_number  AS billing_account_number,
    driveload_financial_metrics_program_phase_1_source.dim_crm_account_id,
    driveload_financial_metrics_program_phase_1_source.dim_parent_crm_account_id,
    driveload_financial_metrics_program_phase_1_source.parent_crm_account_name,
    driveload_financial_metrics_program_phase_1_source.parent_crm_account_billing_country,
    CASE
      WHEN driveload_financial_metrics_program_phase_1_source.parent_crm_account_sales_segment IS NULL THEN 'SMB'
      WHEN driveload_financial_metrics_program_phase_1_source.parent_crm_account_sales_segment = 'Pubsec' THEN 'PubSec'
      ELSE driveload_financial_metrics_program_phase_1_source.parent_crm_account_sales_segment
    END                                                                      AS parent_crm_account_sales_segment,
    driveload_financial_metrics_program_phase_1_source.parent_crm_account_industry,
    NULL                                                                     AS parent_crm_account_geo,
    driveload_financial_metrics_program_phase_1_source.parent_crm_account_owner_team,
    driveload_financial_metrics_program_phase_1_source.parent_crm_account_sales_territory,
    NULL                                                                     AS dim_subscription_id,
    driveload_financial_metrics_program_phase_1_source.subscription_name,
    driveload_financial_metrics_program_phase_1_source.subscription_status,
    driveload_financial_metrics_program_phase_1_source.subscription_sales_type,
    driveload_financial_metrics_program_phase_1_source.product_name,
    NULL                                                                     AS product_rate_plan_name,
    NULL                                                                     AS product_rate_plan_charge_name,
    NULL                                                                     AS product_deployment_type,
    driveload_financial_metrics_program_phase_1_source.product_category      AS product_tier_name,
    CASE
      WHEN driveload_financial_metrics_program_phase_1_source.delivery = 'Others' THEN 'SaaS'
      ELSE driveload_financial_metrics_program_phase_1_source.delivery
    END                                                                      AS product_delivery_type,
    NULL                                                                     AS product_ranking,
    driveload_financial_metrics_program_phase_1_source.service_type,
    driveload_financial_metrics_program_phase_1_source.unit_of_measure,
    driveload_financial_metrics_program_phase_1_source.mrr,
    driveload_financial_metrics_program_phase_1_source.arr,
    driveload_financial_metrics_program_phase_1_source.quantity,
    NULL                                                                     AS is_arpu,
    NULL                                                                     AS dim_charge_id,
    /*
      The is_licensed_user is not available in the driveload file. We can use the product_tier_name to fill in the historical data.
      This is the same logic found in prep_product_detail.
      */
    CASE
      WHEN product_tier_name = 'Storage' THEN FALSE
      WHEN product_tier_name = 'Other' THEN FALSE
      ELSE TRUE
    END                                                                      AS is_licensed_user,
    driveload_financial_metrics_program_phase_1_source.parent_account_cohort_month,
    driveload_financial_metrics_program_phase_1_source.months_since_parent_account_cohort_start,
    driveload_financial_metrics_program_phase_1_source.parent_crm_account_employee_count_band
  FROM driveload_financial_metrics_program_phase_1_source
  WHERE arr_month <= '2021-06-01'

),

parent_cohort_month_snapshot AS (

  SELECT
    dim_parent_crm_account_id,
    MIN(arr_month) AS parent_account_cohort_month
  FROM mart_arr_snapshot_model_combined
  {{ dbt_utils.group_by(n=1) }}

),

snapshot_model AS (

  SELECT
    mart_arr_snapshot_model_combined.is_arr_month_finalized,
    mart_arr_snapshot_model_combined.arr_month,
    mart_arr_snapshot_model_combined.fiscal_quarter_name_fy,
    mart_arr_snapshot_model_combined.fiscal_year,
    mart_arr_snapshot_model_combined.subscription_start_month,
    mart_arr_snapshot_model_combined.subscription_end_month,
    mart_arr_snapshot_model_combined.dim_billing_account_id,
    mart_arr_snapshot_model_combined.sold_to_country,
    mart_arr_snapshot_model_combined.billing_account_name,
    mart_arr_snapshot_model_combined.billing_account_number,
    mart_arr_snapshot_model_combined.dim_crm_account_id,
    mart_arr_snapshot_model_combined.dim_parent_crm_account_id,
    mart_arr_snapshot_model_combined.parent_crm_account_name,
    mart_arr_snapshot_model_combined.parent_crm_account_billing_country,
    CASE
      WHEN mart_arr_snapshot_model_combined.parent_crm_account_sales_segment IS NULL THEN 'SMB'
      WHEN mart_arr_snapshot_model_combined.parent_crm_account_sales_segment = 'Pubsec' THEN 'PubSec'
      ELSE mart_arr_snapshot_model_combined.parent_crm_account_sales_segment
    END                                                                                                                   AS parent_crm_account_sales_segment,
    mart_arr_snapshot_model_combined.parent_crm_account_industry,
    mart_arr_snapshot_model_combined.parent_crm_account_geo,
    mart_arr_snapshot_model_combined.parent_crm_account_owner_team,
    mart_arr_snapshot_model_combined.parent_crm_account_sales_territory,
    mart_arr_snapshot_model_combined.dim_subscription_id,
    mart_arr_snapshot_model_combined.subscription_name,
    mart_arr_snapshot_model_combined.subscription_status,
    mart_arr_snapshot_model_combined.subscription_sales_type,
    CASE
      WHEN mart_arr_snapshot_model_combined.product_tier_name = 'Self-Managed - Ultimate' THEN 'Ultimate'
      WHEN mart_arr_snapshot_model_combined.product_tier_name = 'Dedicated - Ultimate' THEN 'Ultimate'
      WHEN mart_arr_snapshot_model_combined.product_tier_name = 'Self-Managed - Premium' THEN 'Premium'
      WHEN mart_arr_snapshot_model_combined.product_tier_name = 'Self-Managed - Starter' THEN 'Bronze/Starter'
      WHEN mart_arr_snapshot_model_combined.product_tier_name = 'SaaS - Ultimate' THEN 'Ultimate'
      WHEN mart_arr_snapshot_model_combined.product_tier_name = 'SaaS - Premium' THEN 'Premium'
      WHEN mart_arr_snapshot_model_combined.product_tier_name = 'SaaS - Bronze' THEN 'Bronze/Starter'
      ELSE mart_arr_snapshot_model_combined.product_tier_name
    END                                                                                                                   AS product_name,
    mart_arr_snapshot_model_combined.product_rate_plan_name,
    mart_arr_snapshot_model_combined.product_rate_plan_charge_name,
    mart_arr_snapshot_model_combined.product_deployment_type,
    mart_arr_snapshot_model_combined.product_tier_name,
    CASE
      WHEN mart_arr_snapshot_model_combined.product_delivery_type = 'Others' THEN 'SaaS'
      ELSE mart_arr_snapshot_model_combined.product_delivery_type
    END                                                                                                                   AS product_delivery_type,
    mart_arr_snapshot_model_combined.product_ranking,
    mart_arr_snapshot_model_combined.service_type,
    mart_arr_snapshot_model_combined.unit_of_measure,
    mart_arr_snapshot_model_combined.mrr,
    mart_arr_snapshot_model_combined.arr,
    mart_arr_snapshot_model_combined.quantity,
    mart_arr_snapshot_model_combined.is_arpu,
    mart_arr_snapshot_model_combined.dim_charge_id,
    /*
      The is_licensed_user flag was added in 2022-08-01 to the mart_arr and mart_arr_snapshot_model_combined models. There is no historical data for the is_licensed_user
      flag prior to 2022-08-01. We can use the product_tier_name to fill in the historical data. This is the same logic found in prep_product_detail.
      */
    CASE
      WHEN mart_arr_snapshot_model_combined.is_licensed_user IS NOT NULL
        THEN mart_arr_snapshot_model_combined.is_licensed_user
      WHEN mart_arr_snapshot_model_combined.product_tier_name = 'Storage'
        THEN FALSE
      WHEN mart_arr_snapshot_model_combined.product_tier_name = 'Other'
        THEN FALSE
      ELSE TRUE
    END                                                                                                                   AS is_licensed_user,
    parent_cohort_month_snapshot.parent_account_cohort_month,
    DATEDIFF(MONTH, parent_cohort_month_snapshot.parent_account_cohort_month, mart_arr_snapshot_model_combined.arr_month) AS months_since_parent_account_cohort_start,
    mart_arr_snapshot_model_combined.parent_crm_account_employee_count_band
  FROM mart_arr_snapshot_model_combined
  INNER JOIN snapshot_dates
    ON mart_arr_snapshot_model_combined.arr_month = snapshot_dates.first_day_of_month
      AND mart_arr_snapshot_model_combined.snapshot_date = snapshot_dates.snapshot_date_fpa
  --calculate parent cohort month based on correct cohort logic
  LEFT JOIN parent_cohort_month_snapshot
    ON mart_arr_snapshot_model_combined.dim_parent_crm_account_id = parent_cohort_month_snapshot.dim_parent_crm_account_id
  WHERE mart_arr_snapshot_model_combined.is_jihu_account != 'TRUE'
    AND mart_arr_snapshot_model_combined.arr_month >= '2021-07-01'

),

combined AS (

  SELECT *
  FROM snapshot_model

  UNION ALL

  SELECT *
  FROM phase_one

),

parent_arr AS (

  SELECT
    arr_month,
    dim_parent_crm_account_id,
    SUM(arr) AS arr
  FROM combined
  GROUP BY 1, 2

),

parent_arr_band_calc AS (

  SELECT
    arr_month,
    dim_parent_crm_account_id,
    CASE
      WHEN arr > 5000 THEN 'ARR > $5K'
      WHEN arr <= 5000 THEN 'ARR <= $5K'
    END AS arr_band_calc
  FROM parent_arr

),

edu_subscriptions AS (

  /*
    The is_arpu flag was added in 2022-08-01 to the mart_arr and mart_arr_snapshot_model_combined models. There is no historical data for the is_arpu
    flag prior to 2022-08-01. Moreover, the required product_rate_plan_name is not in the driveload financial metrics file to build out the flag.
    Therefore, we can search for the subscriptions themselves to flag the EDU subscriptions and use the product_tier_name to flag the storage
    related charges to fill in the historical data.
    */
  SELECT DISTINCT subscription_name
  FROM mart_arr_snapshot_bottom_up
  WHERE product_rate_plan_name LIKE '%EDU%'
    AND arr_month <= '2022-07-01'

),

intermediate AS (
  --Snap in arr_band_calc based on correct logic. Some historical in mart_arr_snapshot_model_combined do not have the arr_band_calc.
  SELECT
    combined.arr_month,
    combined.is_arr_month_finalized,
    combined.fiscal_quarter_name_fy,
    combined.fiscal_year,
    combined.subscription_start_month,
    combined.subscription_end_month,
    combined.dim_billing_account_id,
    combined.sold_to_country,
    combined.billing_account_name,
    combined.billing_account_number,
    combined.dim_crm_account_id,
    combined.dim_parent_crm_account_id,
    combined.parent_crm_account_name,
    combined.parent_crm_account_billing_country,
    combined.parent_crm_account_sales_segment,
    combined.parent_crm_account_industry,
    combined.parent_crm_account_geo,
    combined.parent_crm_account_owner_team,
    combined.parent_crm_account_sales_territory,
    combined.dim_subscription_id,
    combined.subscription_name,
    combined.subscription_status,
    combined.subscription_sales_type,
    combined.product_name,
    CASE
      WHEN combined.product_name NOT IN ('Ultimate', 'Premium', 'Bronze/Starter')
        THEN 'All Others'
      ELSE combined.product_name
    END                                                                    AS product_name_grouped,
    combined.product_rate_plan_name,
    combined.product_rate_plan_charge_name,
    combined.product_deployment_type,
    combined.product_tier_name,
    combined.product_delivery_type,
    combined.product_ranking,
    combined.service_type,
    combined.unit_of_measure,
    combined.mrr,
    combined.arr,
    combined.quantity,
    --This logic fills in the missing data and uses the core logic found in prep_product_detail to make the is_arpu flag.
    CASE
      WHEN combined.is_arpu IS NOT NULL
        THEN combined.is_arpu
      WHEN combined.product_tier_name = 'Storage'
        THEN FALSE
      WHEN combined.product_rate_plan_name LIKE '%EDU%'
        THEN FALSE
      WHEN edu_subscriptions.subscription_name IS NOT NULL
        THEN FALSE
      ELSE TRUE
    END                                                                    AS is_arpu,
    combined.dim_charge_id,
    combined.is_licensed_user,
    combined.parent_account_cohort_month,
    combined.months_since_parent_account_cohort_start,
    COALESCE(parent_arr_band_calc.arr_band_calc, 'Missing crm_account_id') AS arr_band_calc,
    combined.parent_crm_account_employee_count_band
  FROM combined
  LEFT JOIN parent_arr_band_calc
    ON combined.dim_parent_crm_account_id = parent_arr_band_calc.dim_parent_crm_account_id
      AND combined.arr_month = parent_arr_band_calc.arr_month
  LEFT JOIN edu_subscriptions
    ON combined.subscription_name = edu_subscriptions.subscription_name

),

segment_ranking AS (
  SELECT
    *,
    (CASE WHEN parent_crm_account_sales_segment = 'Large' THEN 4
      WHEN parent_crm_account_sales_segment = 'Mid-Market' THEN 3
      WHEN parent_crm_account_sales_segment = 'SMB' THEN 2
      WHEN parent_crm_account_sales_segment = 'PubSec' THEN 1
    END) AS segment_ranking
  FROM intermediate
),

segment_maxed AS (
  SELECT
    dim_parent_crm_account_id,
    MAX(segment_ranking) AS segment_ranked,
    (CASE WHEN segment_ranked = 4 THEN 'Large'
      WHEN segment_ranked = 3 THEN 'Mid-Market'
      WHEN segment_ranked = 2 THEN 'SMB'
      WHEN segment_ranked = 1 THEN 'PubSec'
    END)                 AS segment
  FROM segment_ranking
  GROUP BY 1
),

segment AS (
  SELECT
    intermediate.*,
    segment_maxed.segment AS segment_modified
  FROM intermediate
  LEFT JOIN segment_maxed
    ON intermediate.dim_parent_crm_account_id = segment_maxed.dim_parent_crm_account_id
),

final AS (
  SELECT
    *,
    CASE
      WHEN product_name ILIKE '%Premium%' THEN 'Premium'
      WHEN COALESCE(product_rate_plan_charge_name, product_rate_plan_name) ILIKE '%duo%' THEN 'Duo Pro'
      WHEN COALESCE(product_rate_plan_charge_name, product_rate_plan_name) ILIKE '%Success%Plan%Services%' THEN 'Success Plan Services'
      WHEN COALESCE(product_rate_plan_charge_name, product_rate_plan_name) ILIKE '%Dedicate%' THEN 'Dedicated-Ultimate'
      WHEN product_name ILIKE '%Ultimate%' THEN 'Ultimate'
      WHEN COALESCE(product_rate_plan_charge_name, product_rate_plan_name) ILIKE '%Enterprise%Agile%Planning%' THEN 'Enterprise Agile Planning'
      WHEN COALESCE(product_rate_plan_charge_name, product_rate_plan_name) ILIKE '%Gitlab%Storage%' THEN 'Storage'
      WHEN COALESCE(product_rate_plan_charge_name, product_rate_plan_name) ILIKE '%Assigned Support Engineer%' THEN 'Assigned Support Engineer'
      WHEN product_name ILIKE '%Bronze%'
        OR product_tier_name ILIKE '%Starter%' THEN 'Bronze/Starter'
      ELSE 'Others'
    END                              AS product_name_modified,
    CASE WHEN product_delivery_type = 'Not Applicable'
        THEN
          (CASE WHEN product_rate_plan_name LIKE 'SaaS%' THEN 'SaaS'
            WHEN product_rate_plan_name LIKE 'Self-Managed%' THEN 'Self-Managed'
            WHEN product_rate_plan_name LIKE 'Dedicated%' THEN 'SaaS'
          END)
      ELSE product_delivery_type END
      AS product_delivery_type_modified,
    MAX(arr_month) OVER () AS max_arr_month
  FROM segment
)

SELECT
  *
FROM final
