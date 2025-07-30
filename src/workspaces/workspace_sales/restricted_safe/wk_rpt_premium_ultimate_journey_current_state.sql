{{
  config(
    materialized='table'
    )
}}

{{ simple_cte([
    ('mart_charge', 'mart_charge'),
    ('mart_arr', 'mart_arr'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('mart_crm_task', 'mart_crm_task'),
    ('mart_crm_event', 'mart_crm_event'),
    ('rpt_l2r_campaign_interactions_paid_account', 'rpt_l2r_campaign_interactions_paid_account'),
    ('rpt_product_usage_marketing', 'rpt_product_usage_marketing'),
    ('rpt_product_usage_health_score', 'rpt_product_usage_health_score')
]) }}

, latest_subscription_versions AS (
  SELECT 
    dim_subscription_id_original,
    MAX(effective_start_date) AS latest_effective_date
  FROM mart_charge
  WHERE is_included_in_arr_calc = TRUE
    AND (product_tier_name LIKE '%Premium%' OR product_tier_name LIKE '%Ultimate%')
    AND product_category = 'Base Products'
  GROUP BY dim_subscription_id_original
)

, subscription_tier_changes AS (
  SELECT 
    dim_crm_account_id,
    dim_subscription_id_original,
    product_tier_name,
    product_deployment_type,
    effective_start_date,
    -- Use row_number to get the first occurrence of each tier for each subscription
    ROW_NUMBER() OVER (
      PARTITION BY dim_crm_account_id, dim_subscription_id_original, 
      CASE 
        WHEN product_tier_name LIKE '%Premium%' THEN 'Premium'
        WHEN product_tier_name LIKE '%Ultimate%' THEN 'Ultimate'
        ELSE product_tier_name
      END
      ORDER BY effective_start_date
    ) AS tier_sequence
  FROM mart_charge
  WHERE is_included_in_arr_calc = TRUE
    AND (product_tier_name LIKE '%Premium%' OR product_tier_name LIKE '%Ultimate%')
    AND product_category = 'Base Products'
    AND effective_start_date != effective_end_date
    AND arr > 0
)

-- Get all subscription dates by deployment type
, subscription_dates AS (
  SELECT 
    dim_crm_account_id,
    dim_subscription_id_original,
    product_deployment_type,
    DATE(MIN(CASE WHEN product_tier_name LIKE '%Premium%' THEN effective_start_date ELSE NULL END)) AS first_premium_date,
    DATE(MAX(CASE WHEN product_tier_name LIKE '%Premium%' THEN effective_start_date ELSE NULL END)) AS latest_premium_date,
    DATE(MIN(CASE WHEN product_tier_name LIKE '%Ultimate%' THEN effective_start_date ELSE NULL END)) AS first_ultimate_date,
    DATE(MAX(CASE WHEN product_tier_name LIKE '%Ultimate%' THEN effective_start_date ELSE NULL END)) AS latest_ultimate_date,
    -- Calculate days for individual subscription upgrades
    CASE 
      WHEN MIN(CASE WHEN product_tier_name LIKE '%Premium%' THEN effective_start_date ELSE NULL END) IS NOT NULL
       AND MIN(CASE WHEN product_tier_name LIKE '%Ultimate%' THEN effective_start_date ELSE NULL END) IS NOT NULL
       AND MIN(CASE WHEN product_tier_name LIKE '%Premium%' THEN effective_start_date ELSE NULL END) < 
          MIN(CASE WHEN product_tier_name LIKE '%Ultimate%' THEN effective_start_date ELSE NULL END)
      THEN DATEDIFF(day, 
                   MIN(CASE WHEN product_tier_name LIKE '%Premium%' THEN effective_start_date ELSE NULL END),
                   MIN(CASE WHEN product_tier_name LIKE '%Ultimate%' THEN effective_start_date ELSE NULL END))
      ELSE NULL
    END AS subscription_upgrade_days,
    -- Flag subscriptions that upgraded
    CASE 
      WHEN MIN(CASE WHEN product_tier_name LIKE '%Premium%' THEN effective_start_date ELSE NULL END) IS NOT NULL
       AND MIN(CASE WHEN product_tier_name LIKE '%Ultimate%' THEN effective_start_date ELSE NULL END) IS NOT NULL
       AND MIN(CASE WHEN product_tier_name LIKE '%Premium%' THEN effective_start_date ELSE NULL END) < 
          MIN(CASE WHEN product_tier_name LIKE '%Ultimate%' THEN effective_start_date ELSE NULL END)
      THEN 1
      ELSE 0
    END AS is_upgrade_subscription
  FROM subscription_tier_changes
  GROUP BY dim_crm_account_id, dim_subscription_id_original, product_deployment_type
)

-- Get first occurrences of Premium and Ultimate per account/deployment
, first_tier_dates AS (
  SELECT
    dim_crm_account_id,
    product_deployment_type,
    MIN(CASE WHEN product_tier_name LIKE '%Premium%' THEN effective_start_date END) AS first_premium_date,
    MIN(CASE WHEN product_tier_name LIKE '%Ultimate%' THEN effective_start_date END) AS first_ultimate_date
  FROM mart_charge
  WHERE is_included_in_arr_calc = TRUE
    AND (product_tier_name LIKE '%Premium%' OR product_tier_name LIKE '%Ultimate%')
    AND product_category = 'Base Products'
    AND effective_start_date != effective_end_date
    AND arr > 0
  GROUP BY 
    dim_crm_account_id,
    product_deployment_type
)

-- Create a reference table with all Ultimate purchase dates by account
, ultimate_purchase_dates AS (
  SELECT
    dim_crm_account_id,
    DATE(effective_start_date) AS ultimate_purchase_date
  FROM mart_charge
  WHERE is_included_in_arr_calc = TRUE
    AND product_tier_name LIKE '%Ultimate%'
    AND product_category = 'Base Products'
    AND effective_start_date != effective_end_date
    AND arr > 0
    AND EXISTS (
      SELECT 1 FROM first_tier_dates
      WHERE first_tier_dates.dim_crm_account_id = mart_charge.dim_crm_account_id
        AND first_tier_dates.product_deployment_type = mart_charge.product_deployment_type
        AND first_tier_dates.first_ultimate_date = mart_charge.effective_start_date
    )
)

-- Identify license expansions using mart_charge - with additional filters
, license_expansion AS (
  SELECT
    dim_crm_account_id,
    dim_subscription_id_original,
    product_tier_name,
    effective_start_date,
    quantity,
    -- Get previous quantity and date from the same subscription
    LAG(quantity) OVER (
      PARTITION BY dim_crm_account_id, dim_subscription_id_original 
      ORDER BY effective_start_date
    ) AS previous_quantity,
    LAG(effective_start_date) OVER (
      PARTITION BY dim_crm_account_id, dim_subscription_id_original 
      ORDER BY effective_start_date
    ) AS previous_effective_date,
    -- Identify true expansions: quantity increased AND date is different AND significant increase (>10%)
    CASE 
      WHEN quantity > COALESCE(LAG(quantity) OVER (
        PARTITION BY dim_crm_account_id, dim_subscription_id_original 
        ORDER BY effective_start_date
      ), 0) 
      AND effective_start_date > COALESCE(LAG(effective_start_date) OVER (
        PARTITION BY dim_crm_account_id, dim_subscription_id_original 
        ORDER BY effective_start_date
      ), '1900-01-01'::date)
      -- Add a minimum threshold for expansion (e.g., at least 10% increase)
      AND quantity >= COALESCE(LAG(quantity) OVER (
        PARTITION BY dim_crm_account_id, dim_subscription_id_original 
        ORDER BY effective_start_date
      ), 0) * 1.10
      THEN TRUE
      ELSE FALSE
    END AS is_quantity_increase
  FROM mart_charge
  WHERE is_included_in_arr_calc = TRUE
    AND (product_tier_name LIKE '%Premium%' OR product_tier_name LIKE '%Ultimate%')
    AND product_category = 'Base Products'
    AND effective_start_date != effective_end_date
    AND quantity IS NOT NULL
)

-- Get first expansion dates by tier - ensuring effective date is greater than previous
, expansion_dates AS (
  SELECT
    dim_crm_account_id,
    DATE(MIN(CASE 
      WHEN is_quantity_increase = TRUE 
      AND product_tier_name LIKE '%Premium%'
      AND effective_start_date > previous_effective_date -- Explicit check that date is greater
      THEN effective_start_date
    END)) AS first_premium_expansion_date,
    DATE(MIN(CASE 
      WHEN is_quantity_increase = TRUE 
      AND product_tier_name LIKE '%Ultimate%'
      AND effective_start_date > previous_effective_date -- Explicit check that date is greater 
      THEN effective_start_date
    END)) AS first_ultimate_expansion_date
  FROM license_expansion
  GROUP BY dim_crm_account_id
)

-- Use latest subscription versions for current tier assessment
, latest_tier_by_subscription AS (
  SELECT
    mc.dim_crm_account_id,
    mc.dim_subscription_id_original,
    mc.product_tier_name,
    mc.product_deployment_type,
    mc.effective_start_date,
    mc.quantity,
    mc.arr
  FROM mart_charge mc
  JOIN latest_subscription_versions lsv
    ON mc.dim_subscription_id_original = lsv.dim_subscription_id_original
    AND mc.effective_start_date = lsv.latest_effective_date
  WHERE mc.is_included_in_arr_calc = TRUE
    AND (mc.product_tier_name LIKE '%Premium%' OR mc.product_tier_name LIKE '%Ultimate%')
    AND mc.product_category = 'Base Products'
)

-- Aggregate to account level across all deployment types
, account_journey AS (
  SELECT
    dim_crm_account_id,
    DATE(MIN(first_premium_date)) AS first_premium_date,
    DATE(MAX(latest_premium_date)) AS latest_premium_date,
    DATE(MIN(first_ultimate_date)) AS first_ultimate_date,
    DATE(MAX(latest_ultimate_date)) AS latest_ultimate_date,
    LISTAGG(DISTINCT product_deployment_type, ', ') AS deployment_types,
    COUNT(DISTINCT product_deployment_type) AS deployment_type_count,
    -- Find subscriptions that upgraded
    SUM(is_upgrade_subscription) AS subscriptions_upgraded_count,
    -- Get the fastest individual subscription upgrade
    MIN(CASE WHEN subscription_upgrade_days IS NOT NULL THEN subscription_upgrade_days ELSE NULL END) AS fastest_subscription_upgrade_days
  FROM subscription_dates
  GROUP BY dim_crm_account_id
)

-- Get details by deployment type
, deployment_details AS (
  SELECT
    dim_crm_account_id,
    product_deployment_type,
    DATE(MIN(first_premium_date)) AS first_premium_date_by_deployment,
    DATE(MAX(latest_premium_date)) AS latest_premium_date_by_deployment,
    DATE(MIN(first_ultimate_date)) AS first_ultimate_date_by_deployment,
    DATE(MAX(latest_ultimate_date)) AS latest_ultimate_date_by_deployment
  FROM subscription_dates
  GROUP BY dim_crm_account_id, product_deployment_type
)

-- Get account names
, account_names AS (
  SELECT DISTINCT 
    dim_crm_account_id, 
    crm_account_name
  FROM mart_charge
  WHERE product_category = 'Base Products'
)

-- Get Ultimate campaign interactions
, ultimate_campaign_interactions AS (
  SELECT 
    dim_crm_account_id,
    DATE(MIN(bizible_touchpoint_date)) AS first_ultimate_campaign_date,
    DATE(MAX(bizible_touchpoint_date)) AS latest_ultimate_campaign_date,
    COUNT(*) AS ultimate_campaign_touchpoint_count,
    LISTAGG(DISTINCT gtm_motion, ', ') AS ultimate_campaign_motions
  FROM rpt_l2r_campaign_interactions_paid_account
  WHERE touchpoint_type = 'Person Touchpoint'
  AND (
    gtm_motion = 'Security & Compliance' 
    OR gtm_motion = 'DevSecOps Platform'
  )
  GROUP BY dim_crm_account_id
)

-- Get Ultimate trial information
, ultimate_trial_info AS (
  SELECT
    dim_crm_account_id,
    DATE(MIN(CASE WHEN event_name = 'Trial' AND trial_type LIKE '%Ultimate%' THEN event_date END)) AS first_ultimate_trial_date,
    DATE(MIN(CASE WHEN event_name = 'Trial' AND trial_type LIKE '%Ultimate%' THEN trial_end_date END)) AS first_ultimate_trial_end_date
  FROM rpt_product_usage_marketing
  WHERE event_name = 'Trial' 
    AND trial_type LIKE '%Ultimate%'
  GROUP BY dim_crm_account_id
)

-- Get sales opportunity dates
, opportunity_dates AS (
  SELECT
    dim_crm_account_id,
    -- Premium opportunity dates
    DATE(MIN(CASE 
          WHEN LOWER(opportunity_name) LIKE '%premium%' 
          THEN created_date 
        END)) AS first_premium_opportunity_created_date,
    DATE(MIN(CASE 
          WHEN LOWER(opportunity_name) LIKE '%premium%' AND is_closed_won = TRUE
          THEN close_date 
        END)) AS first_premium_opportunity_close_date,
    -- Ultimate opportunity dates  
    DATE(MIN(CASE 
          WHEN LOWER(opportunity_name) LIKE '%ultimate%' 
          THEN created_date 
        END)) AS first_ultimate_opportunity_created_date,
    DATE(MIN(CASE 
          WHEN LOWER(opportunity_name) LIKE '%ultimate%' AND is_closed_won = TRUE
          THEN close_date 
        END)) AS first_ultimate_opportunity_close_date
  FROM mart_crm_opportunity
  GROUP BY dim_crm_account_id
)

-- Get tasks and activities - MODIFIED to explicitly check before/after Ultimate Purchase
, account_tasks AS (
  SELECT
    mart_crm_task.dim_crm_account_id,
    -- Basic metrics
    COUNT(CASE WHEN mart_crm_task.sa_activity_type IS NOT NULL THEN mart_crm_task.dim_crm_task_pk END) AS sa_activity_count,
    DATE(MIN(CASE WHEN mart_crm_task.sa_activity_type IS NOT NULL THEN mart_crm_task.task_date END)) AS first_sa_activity_date,
    COUNT(CASE WHEN mart_crm_task.is_demo_task = TRUE AND mart_crm_task.task_status = 'Completed' THEN mart_crm_task.dim_crm_task_pk END) AS completed_demo_count,
    DATE(MIN(CASE WHEN mart_crm_task.is_demo_task = TRUE AND mart_crm_task.task_status = 'Completed' THEN COALESCE(mart_crm_task.task_completed_date, mart_crm_task.task_date) END)) AS first_demo_completed_date,
    
    -- BEFORE ULTIMATE PURCHASE: SA Activity dates for consideration (explicitly before Ultimate Purchase)
    DATE(MIN(CASE 
              WHEN mart_crm_task.sa_activity_type IS NOT NULL 
               AND NOT EXISTS (
                 SELECT 1 
                 FROM ultimate_purchase_dates upd
                 WHERE upd.dim_crm_account_id = mart_crm_task.dim_crm_account_id
                   AND upd.ultimate_purchase_date <= DATE(mart_crm_task.task_date)
               )
              THEN mart_crm_task.task_date 
            END)) AS sa_activity_before_ultimate_purchase_date,
    
    -- AFTER ULTIMATE PURCHASE: SA Activity dates for onboarding (explicitly after Ultimate Purchase)
    DATE(MIN(CASE 
              WHEN mart_crm_task.sa_activity_type IS NOT NULL 
               AND EXISTS (
                 SELECT 1 
                 FROM ultimate_purchase_dates upd
                 WHERE upd.dim_crm_account_id = mart_crm_task.dim_crm_account_id
                   AND upd.ultimate_purchase_date <= DATE(mart_crm_task.task_date)
               )
              THEN mart_crm_task.task_date 
            END)) AS sa_activity_after_ultimate_purchase_date,
    
    -- BEFORE ULTIMATE PURCHASE: Demo completion dates for consideration (explicitly before Ultimate Purchase)
    DATE(MIN(CASE 
              WHEN mart_crm_task.is_demo_task = TRUE 
               AND mart_crm_task.task_status = 'Completed'
               AND NOT EXISTS (
                 SELECT 1 
                 FROM ultimate_purchase_dates upd
                 WHERE upd.dim_crm_account_id = mart_crm_task.dim_crm_account_id
                   AND upd.ultimate_purchase_date <= DATE(COALESCE(mart_crm_task.task_completed_date, mart_crm_task.task_date))
               )
              THEN COALESCE(mart_crm_task.task_completed_date, mart_crm_task.task_date) 
            END)) AS demo_completed_before_ultimate_purchase_date,
    
    -- AFTER ULTIMATE PURCHASE: Demo completion dates for onboarding (explicitly after Ultimate Purchase)
    DATE(MIN(CASE 
              WHEN mart_crm_task.is_demo_task = TRUE 
               AND mart_crm_task.task_status = 'Completed'
               AND EXISTS (
                 SELECT 1 
                 FROM ultimate_purchase_dates upd
                 WHERE upd.dim_crm_account_id = mart_crm_task.dim_crm_account_id
                   AND upd.ultimate_purchase_date <= DATE(COALESCE(mart_crm_task.task_completed_date, mart_crm_task.task_date))
               )
              THEN COALESCE(mart_crm_task.task_completed_date, mart_crm_task.task_date) 
            END)) AS demo_completed_after_ultimate_purchase_date,
    
    -- Count of SA Activities before/after Ultimate Purchase
    COUNT(CASE 
            WHEN mart_crm_task.sa_activity_type IS NOT NULL 
             AND NOT EXISTS (
               SELECT 1 
               FROM ultimate_purchase_dates upd
               WHERE upd.dim_crm_account_id = mart_crm_task.dim_crm_account_id
                 AND upd.ultimate_purchase_date <= DATE(mart_crm_task.task_date)
             )
            THEN mart_crm_task.dim_crm_task_pk 
          END) AS sa_activity_before_ultimate_purchase_count,
          
    COUNT(CASE 
            WHEN mart_crm_task.sa_activity_type IS NOT NULL 
             AND EXISTS (
               SELECT 1 
               FROM ultimate_purchase_dates upd
               WHERE upd.dim_crm_account_id = mart_crm_task.dim_crm_account_id
                 AND upd.ultimate_purchase_date <= DATE(mart_crm_task.task_date)
             )
            THEN mart_crm_task.dim_crm_task_pk 
          END) AS sa_activity_after_ultimate_purchase_count,
    
    -- Count of Demo Completions before/after Ultimate Purchase
    COUNT(CASE 
            WHEN mart_crm_task.is_demo_task = TRUE 
             AND mart_crm_task.task_status = 'Completed'
             AND NOT EXISTS (
               SELECT 1 
               FROM ultimate_purchase_dates upd
               WHERE upd.dim_crm_account_id = mart_crm_task.dim_crm_account_id
                 AND upd.ultimate_purchase_date <= DATE(COALESCE(mart_crm_task.task_completed_date, mart_crm_task.task_date))
             )
            THEN mart_crm_task.dim_crm_task_pk 
          END) AS demo_completed_before_ultimate_purchase_count,
          
    COUNT(CASE 
            WHEN mart_crm_task.is_demo_task = TRUE 
             AND mart_crm_task.task_status = 'Completed'
             AND EXISTS (
               SELECT 1 
               FROM ultimate_purchase_dates upd
               WHERE upd.dim_crm_account_id = mart_crm_task.dim_crm_account_id
                 AND upd.ultimate_purchase_date <= DATE(COALESCE(mart_crm_task.task_completed_date, mart_crm_task.task_date))
             )
            THEN mart_crm_task.dim_crm_task_pk 
          END) AS demo_completed_after_ultimate_purchase_count,
    
    -- SA Activity after Premium (keeping this existing metric)
    COUNT(CASE 
            WHEN mart_crm_task.sa_activity_type IS NOT NULL 
             AND mart_crm_task.task_date > account_journey.first_premium_date
            THEN mart_crm_task.dim_crm_task_pk 
          END) AS sa_activity_after_premium_count
  FROM mart_crm_task
  JOIN account_journey ON mart_crm_task.dim_crm_account_id = account_journey.dim_crm_account_id
  GROUP BY mart_crm_task.dim_crm_account_id, account_journey.first_premium_date
)

-- Get events information
, account_events AS (
  SELECT
    mart_crm_event.dim_crm_account_id,
    -- First event of any type
    DATE(MIN(mart_crm_event.created_at)) AS first_event_created_date,
    -- First Ultimate event (using keywords in subject or description)
    DATE(MIN(CASE 
          WHEN (LOWER(mart_crm_event.event_subject) LIKE '%ultimate%' 
                OR LOWER(mart_crm_event.event_subject) LIKE '%security%'
                OR LOWER(mart_crm_event.event_description) LIKE '%ultimate%'
                OR LOWER(mart_crm_event.event_description) LIKE '%security%')
          THEN mart_crm_event.created_at 
        END)) AS first_ultimate_event_created_date,
    -- Count events after Premium
    COUNT(CASE 
            WHEN mart_crm_event.created_at > account_journey.first_premium_date 
            THEN mart_crm_event.dim_crm_event_pk 
          END) AS events_after_premium_count,
    -- Count Ultimate events after Premium
    COUNT(CASE 
            WHEN mart_crm_event.created_at > account_journey.first_premium_date 
             AND (LOWER(mart_crm_event.event_subject) LIKE '%ultimate%' 
                  OR LOWER(mart_crm_event.event_subject) LIKE '%security%'
                  OR LOWER(mart_crm_event.event_description) LIKE '%ultimate%'
                  OR LOWER(mart_crm_event.event_description) LIKE '%security%')
            THEN mart_crm_event.dim_crm_event_pk 
          END) AS ultimate_events_after_premium_count
  FROM mart_crm_event
  JOIN account_journey ON mart_crm_event.dim_crm_account_id = account_journey.dim_crm_account_id
  GROUP BY mart_crm_event.dim_crm_account_id, account_journey.first_premium_date
)

-- Get product usage health scores joined with mart_arr to identify tier
-- Simplified to only what's needed for security and premium ramped milestone dates
, product_usage_health_with_tier AS (
  SELECT
    rpt_product_usage_health_score.dim_crm_account_id,
    rpt_product_usage_health_score.dim_namespace_id,
    rpt_product_usage_health_score.dim_subscription_id_original,
    rpt_product_usage_health_score.deployment_type,
    rpt_product_usage_health_score.ping_created_at,
    rpt_product_usage_health_score.license_utilization_color,
    rpt_product_usage_health_score.scm_color,
    rpt_product_usage_health_score.ci_pipeline_utilization_color,
    rpt_product_usage_health_score.security_color_ultimate_only,
    rpt_product_usage_health_score.secure_scanners_utilization,
    rpt_product_usage_health_score.is_primary_instance_subscription,
    mart_arr.product_tier_name
  FROM rpt_product_usage_health_score
  LEFT JOIN mart_arr
    ON mart_arr.arr_month = rpt_product_usage_health_score.snapshot_month
    AND mart_arr.product_deployment_type = rpt_product_usage_health_score.deployment_type
    AND mart_arr.dim_subscription_id_original = rpt_product_usage_health_score.dim_subscription_id_original
  WHERE rpt_product_usage_health_score.is_primary_instance_subscription = TRUE
)

-- Time of security features first usage - creating a simplified approach with explicit before/after logic
, security_features_first_usage_base AS (
  SELECT
    dim_crm_account_id,
    DATE(ping_created_at) AS event_date,
    secure_scanners_utilization,
    security_color_ultimate_only
  FROM product_usage_health_with_tier
  WHERE is_primary_instance_subscription = TRUE
)

, security_features_first_usage AS (
  SELECT
    sf.dim_crm_account_id,
    -- First security feature usage (regardless of timing)
    MIN(CASE WHEN sf.secure_scanners_utilization > 0 THEN sf.event_date END) AS first_security_feature_date,
    -- Security ramped date
    MIN(CASE WHEN sf.security_color_ultimate_only = 'Green' THEN sf.event_date END) AS security_ramped_date
  FROM security_features_first_usage_base sf
  GROUP BY sf.dim_crm_account_id
)

-- BEFORE ULTIMATE PURCHASE: Security features used before Ultimate purchase
, security_features_before_ultimate AS (
  SELECT
    sf.dim_crm_account_id,
    MIN(CASE WHEN sf.secure_scanners_utilization > 0 THEN sf.event_date END) AS security_feature_before_ultimate_purchase_date,
    COUNT(CASE WHEN sf.secure_scanners_utilization > 0 THEN 1 END) AS security_feature_before_ultimate_purchase_count
  FROM security_features_first_usage_base sf
  LEFT JOIN ultimate_purchase_dates upd 
    ON sf.dim_crm_account_id = upd.dim_crm_account_id
  WHERE sf.secure_scanners_utilization > 0
    AND (upd.ultimate_purchase_date IS NULL OR sf.event_date < upd.ultimate_purchase_date)
  GROUP BY sf.dim_crm_account_id
)

-- AFTER ULTIMATE PURCHASE: Security features used after Ultimate purchase
, security_features_after_ultimate AS (
  SELECT
    sf.dim_crm_account_id,
    MIN(CASE WHEN sf.secure_scanners_utilization > 0 THEN sf.event_date END) AS security_feature_after_ultimate_purchase_date,
    COUNT(CASE WHEN sf.secure_scanners_utilization > 0 THEN 1 END) AS security_feature_after_ultimate_purchase_count,
    MIN(CASE WHEN sf.security_color_ultimate_only = 'Green' THEN sf.event_date END) AS security_ramped_after_ultimate_purchase_date
  FROM security_features_first_usage_base sf
  JOIN ultimate_purchase_dates upd 
    ON sf.dim_crm_account_id = upd.dim_crm_account_id
  WHERE sf.secure_scanners_utilization > 0
    AND sf.event_date > upd.ultimate_purchase_date
  GROUP BY sf.dim_crm_account_id
)

-- Time of premium ramped first achieved - using ping_created_at for precise timing
, premium_ramped_first_achieved AS (
  SELECT
    dim_crm_account_id,
    DATE(MIN(CASE 
          WHEN (license_utilization_color = 'Green'
                OR scm_color = 'Green'
                OR ci_pipeline_utilization_color = 'Yellow' 
                OR ci_pipeline_utilization_color = 'Green')
           AND product_tier_name ILIKE '%Premium%' -- Only consider when on Premium tier
          THEN ping_created_at
        END)) AS first_premium_ramped_date
  FROM product_usage_health_with_tier
  WHERE is_primary_instance_subscription = TRUE
  GROUP BY dim_crm_account_id
)

-- Calculate current tier metrics based on latest subscription versions
, current_tier_metrics AS (
  SELECT
    dim_crm_account_id,
    SUM(CASE WHEN product_tier_name LIKE '%Premium%' THEN arr ELSE 0 END) AS current_premium_arr,
    SUM(CASE WHEN product_tier_name LIKE '%Ultimate%' THEN arr ELSE 0 END) AS current_ultimate_arr,
    SUM(CASE WHEN product_tier_name LIKE '%Premium%' THEN quantity ELSE 0 END) AS current_premium_quantity,
    SUM(CASE WHEN product_tier_name LIKE '%Ultimate%' THEN quantity ELSE 0 END) AS current_ultimate_quantity,
    LISTAGG(DISTINCT product_tier_name, ', ') AS current_tiers
  FROM latest_tier_by_subscription
  GROUP BY dim_crm_account_id
)

-- Final data with journey stage classification
, final AS (
  SELECT 
    account_journey.dim_crm_account_id,
    account_names.crm_account_name,
    account_journey.deployment_types,
    account_journey.deployment_type_count,
    account_journey.first_premium_date,
    account_journey.first_ultimate_date,
    
    -- Premium journey details
    premium_ramped_first_achieved.first_premium_ramped_date,
    expansion_dates.first_premium_expansion_date,
    expansion_dates.first_ultimate_expansion_date,
    
    -- Ultimate trial details
    ultimate_trial_info.first_ultimate_trial_date,
    ultimate_trial_info.first_ultimate_trial_end_date,
    
    -- Deployment specific information
    MAX(CASE WHEN deployment_details.product_deployment_type = 'Self-Managed' THEN deployment_details.first_premium_date_by_deployment ELSE NULL END) AS sm_first_premium_date,
    MAX(CASE WHEN deployment_details.product_deployment_type = 'Self-Managed' THEN deployment_details.first_ultimate_date_by_deployment ELSE NULL END) AS sm_first_ultimate_date,
    MAX(CASE WHEN deployment_details.product_deployment_type = 'GitLab.com' THEN deployment_details.first_premium_date_by_deployment ELSE NULL END) AS dotcom_first_premium_date,
    MAX(CASE WHEN deployment_details.product_deployment_type = 'GitLab.com' THEN deployment_details.first_ultimate_date_by_deployment ELSE NULL END) AS dotcom_first_ultimate_date,
    
    -- Sales activity information
    opportunity_dates.first_premium_opportunity_created_date,
    opportunity_dates.first_premium_opportunity_close_date,
    opportunity_dates.first_ultimate_opportunity_created_date,
    opportunity_dates.first_ultimate_opportunity_close_date,
    
    -- Events data
    account_events.first_event_created_date,
    account_events.first_ultimate_event_created_date,
    account_events.events_after_premium_count,
    account_events.ultimate_events_after_premium_count,
    
    -- Tasks and activities data - now with explicit before/after purchase dates
    account_tasks.sa_activity_count,
    account_tasks.first_sa_activity_date,
    account_tasks.sa_activity_before_ultimate_purchase_date,
    account_tasks.sa_activity_after_ultimate_purchase_date,
    account_tasks.sa_activity_before_ultimate_purchase_count,
    account_tasks.sa_activity_after_ultimate_purchase_count,
    account_tasks.completed_demo_count,
    account_tasks.first_demo_completed_date,
    account_tasks.demo_completed_before_ultimate_purchase_date,
    account_tasks.demo_completed_after_ultimate_purchase_date,
    account_tasks.demo_completed_before_ultimate_purchase_count,
    account_tasks.demo_completed_after_ultimate_purchase_count,
    account_tasks.sa_activity_after_premium_count,
    
    -- Campaign interaction data
    ultimate_campaign_interactions.first_ultimate_campaign_date,
    ultimate_campaign_interactions.latest_ultimate_campaign_date,
    ultimate_campaign_interactions.ultimate_campaign_touchpoint_count,
    ultimate_campaign_interactions.ultimate_campaign_motions,
    
    -- Security features usage dates - now with explicit before/after purchase dates
    security_features_first_usage.first_security_feature_date,
    security_features_before_ultimate.security_feature_before_ultimate_purchase_date,
    security_features_after_ultimate.security_feature_after_ultimate_purchase_date,
    security_features_before_ultimate.security_feature_before_ultimate_purchase_count,
    security_features_after_ultimate.security_feature_after_ultimate_purchase_count,
    security_features_first_usage.security_ramped_date,
    security_features_after_ultimate.security_ramped_after_ultimate_purchase_date,
    
    -- Latest subscription version metrics
    current_tier_metrics.current_premium_arr,
    current_tier_metrics.current_ultimate_arr,
    current_tier_metrics.current_premium_quantity,
    current_tier_metrics.current_ultimate_quantity,
    current_tier_metrics.current_tiers,
    
    -- Derived metrics
    CASE
      WHEN account_journey.first_premium_date IS NULL OR account_journey.first_ultimate_date IS NULL THEN NULL
      ELSE DATEDIFF(day, account_journey.first_premium_date, account_journey.first_ultimate_date)
    END AS account_level_days_to_ultimate,
    account_journey.subscriptions_upgraded_count,
    account_journey.fastest_subscription_upgrade_days,
    
    -- Current subscription status - Using the latest subscription versions to determine current tier
    CASE 
      WHEN COALESCE(current_tier_metrics.current_ultimate_arr, 0) > 0 THEN 'Currently Ultimate'
      WHEN COALESCE(current_tier_metrics.current_premium_arr, 0) > 0 THEN 'Currently Premium'
      WHEN account_journey.latest_ultimate_date > COALESCE(account_journey.latest_premium_date, '1900-01-01'::date) OR 
           (account_journey.latest_ultimate_date IS NOT NULL AND account_journey.latest_premium_date IS NULL) 
      THEN 'Currently Ultimate'
      WHEN account_journey.latest_premium_date > COALESCE(account_journey.latest_ultimate_date, '1900-01-01'::date) OR
           (account_journey.latest_premium_date IS NOT NULL AND account_journey.latest_ultimate_date IS NULL)
      THEN 'Currently Premium'
      WHEN account_journey.latest_premium_date = account_journey.latest_ultimate_date
      THEN 'Currently Ultimate'
      ELSE NULL
    END AS current_tier,
    
    -- Journey stage classification based purely on milestone achievements
    -- UPDATED to include Ultimate Expansion stage
    CASE 
      -- Ultimate stages first (if on Ultimate) - Using the current_ultimate_arr to determine if currently on Ultimate
      WHEN (COALESCE(current_tier_metrics.current_ultimate_arr, 0) > 0 OR account_journey.first_ultimate_date IS NOT NULL)
        AND (
          COALESCE(current_tier_metrics.current_ultimate_arr, 0) > 0
          OR account_journey.latest_ultimate_date > COALESCE(account_journey.latest_premium_date, '1900-01-01'::date)
          OR (account_journey.latest_ultimate_date IS NOT NULL AND account_journey.latest_premium_date IS NULL)
        )
      THEN
        CASE
          -- Ultimate Expansion - requires verified expansion date with minimum time gap
          WHEN expansion_dates.first_ultimate_expansion_date IS NOT NULL 
            AND expansion_dates.first_ultimate_expansion_date > account_journey.first_ultimate_date
            AND DATEDIFF(day, account_journey.first_ultimate_date, expansion_dates.first_ultimate_expansion_date) >= 90
          THEN 'Ultimate Expansion'
          
          -- Use security ramped date as the key milestone for Ultimate Ramped
          WHEN security_features_after_ultimate.security_ramped_after_ultimate_purchase_date IS NOT NULL THEN 'Ultimate Ramped'
          
          -- If they have any security feature usage after Ultimate purchase, they're in onboarding
          WHEN security_features_after_ultimate.security_feature_after_ultimate_purchase_date IS NOT NULL 
               OR account_tasks.sa_activity_after_ultimate_purchase_date IS NOT NULL
               OR account_tasks.demo_completed_after_ultimate_purchase_date IS NOT NULL 
               THEN 'Ultimate Onboarding'
               
          -- Otherwise they're just in the purchase stage
          ELSE 'Ultimate Purchase'
        END
      
      -- EXPLICIT CHECK FOR ULTIMATE CONSIDERATION - completely separate from Premium checks
      WHEN (
          ultimate_trial_info.first_ultimate_trial_date IS NOT NULL
          OR opportunity_dates.first_ultimate_opportunity_created_date IS NOT NULL
          OR security_features_before_ultimate.security_feature_before_ultimate_purchase_date IS NOT NULL
          OR COALESCE(ultimate_campaign_interactions.ultimate_campaign_touchpoint_count, 0) > 0
          -- Add before ultimate purchase dates for SA and demo tasks 
          OR account_tasks.sa_activity_before_ultimate_purchase_date IS NOT NULL
          OR account_tasks.demo_completed_before_ultimate_purchase_date IS NOT NULL
        )
      THEN 'Ultimate Consideration'
      
      -- Only evaluate Premium stages if not classified as Ultimate Consideration
      -- Using current_premium_arr to check if currently on Premium
      WHEN (COALESCE(current_tier_metrics.current_premium_arr, 0) > 0 OR account_journey.first_premium_date IS NOT NULL)
      THEN
        CASE
          -- Premium Expansion - requires verified expansion date
          WHEN expansion_dates.first_premium_expansion_date IS NOT NULL 
            AND expansion_dates.first_premium_expansion_date > account_journey.first_premium_date
            AND DATEDIFF(day, account_journey.first_premium_date, expansion_dates.first_premium_expansion_date) >= 90 -- At least 90 days after initial purchase
          THEN 'Premium Expansion'
          
          -- Premium Ramped - use first premium ramped date milestone instead of latest metrics
          WHEN premium_ramped_first_achieved.first_premium_ramped_date IS NOT NULL
          THEN 'Premium Ramped'
          
          -- Default Premium stage - now combines Purchase and Onboarding
          ELSE 'Premium Purchase'
        END
      
      -- Missing data fallbacks based on subscription info rather than product usage
      WHEN COALESCE(current_tier_metrics.current_premium_arr, 0) > 0 OR account_journey.latest_premium_date > COALESCE(account_journey.latest_ultimate_date, '1900-01-01'::date) 
      THEN 'Premium (Based on Subscription)'
      
      WHEN COALESCE(current_tier_metrics.current_ultimate_arr, 0) > 0 OR account_journey.latest_ultimate_date > COALESCE(account_journey.latest_premium_date, '1900-01-01'::date) 
      THEN 'Ultimate (Based on Subscription)'

      -- Catch non-null date based fallbacks
      WHEN account_journey.first_ultimate_date IS NOT NULL
      THEN 'Ultimate (Based on Dates)'
      
      WHEN account_journey.first_premium_date IS NOT NULL
      THEN 'Premium (Based on Dates)'
      
      -- Other cases
      ELSE 'Unknown'
    END AS journey_stage
    
  FROM account_journey
  LEFT JOIN account_names ON account_journey.dim_crm_account_id = account_names.dim_crm_account_id
  LEFT JOIN deployment_details ON account_journey.dim_crm_account_id = deployment_details.dim_crm_account_id
  LEFT JOIN ultimate_campaign_interactions ON account_journey.dim_crm_account_id = ultimate_campaign_interactions.dim_crm_account_id
  LEFT JOIN ultimate_trial_info ON account_journey.dim_crm_account_id = ultimate_trial_info.dim_crm_account_id
  LEFT JOIN opportunity_dates ON account_journey.dim_crm_account_id = opportunity_dates.dim_crm_account_id
  LEFT JOIN account_tasks ON account_journey.dim_crm_account_id = account_tasks.dim_crm_account_id
  LEFT JOIN account_events ON account_journey.dim_crm_account_id = account_events.dim_crm_account_id
  LEFT JOIN security_features_first_usage ON account_journey.dim_crm_account_id = security_features_first_usage.dim_crm_account_id
  LEFT JOIN security_features_before_ultimate ON account_journey.dim_crm_account_id = security_features_before_ultimate.dim_crm_account_id
  LEFT JOIN security_features_after_ultimate ON account_journey.dim_crm_account_id = security_features_after_ultimate.dim_crm_account_id
  LEFT JOIN premium_ramped_first_achieved ON account_journey.dim_crm_account_id = premium_ramped_first_achieved.dim_crm_account_id
  LEFT JOIN expansion_dates ON account_journey.dim_crm_account_id = expansion_dates.dim_crm_account_id
  LEFT JOIN current_tier_metrics ON account_journey.dim_crm_account_id = current_tier_metrics.dim_crm_account_id
  GROUP BY ALL
)

SELECT * FROM final