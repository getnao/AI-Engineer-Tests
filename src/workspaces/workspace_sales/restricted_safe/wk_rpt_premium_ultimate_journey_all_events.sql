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
    ('rpt_product_usage_health_score', 'rpt_product_usage_health_score'),
    ('mart_crm_account', 'mart_crm_account')
]) }}

-- Filter mart_charge to only include past events
, mart_charge_filtered AS (
  SELECT * 
  FROM mart_charge 
  WHERE effective_start_date < CURRENT_DATE()
)

-- Get account metadata
, account_metadata AS (
  SELECT DISTINCT
    mart_charge_filtered.dim_crm_account_id,
    mart_charge_filtered.crm_account_name,
    mart_crm_account.parent_crm_account_name,
    mart_crm_account.parent_crm_account_industry,
    mart_crm_account.parent_crm_account_sales_segment,
    mart_crm_account.parent_crm_account_geo,
    mart_crm_account.parent_crm_account_max_family_employee
  FROM mart_charge_filtered
  LEFT JOIN mart_crm_account
    ON mart_charge_filtered.dim_crm_account_id = mart_crm_account.dim_crm_account_id
  WHERE mart_charge_filtered.product_category = 'Base Products'
)

-- Get latest subscription version for each dim_subscription_id_original
, latest_subscription_versions AS (
  SELECT 
    dim_subscription_id_original,
    MAX(effective_start_date) AS latest_effective_date
  FROM mart_charge_filtered
  WHERE is_included_in_arr_calc = TRUE
    AND (product_tier_name LIKE '%Premium%' OR product_tier_name LIKE '%Ultimate%')
    AND product_category = 'Base Products'
  GROUP BY dim_subscription_id_original
)

-- Get first occurrences of Premium and Ultimate per account/deployment
, first_tier_dates AS (
  SELECT
    dim_crm_account_id,
    product_deployment_type,
    MIN(CASE WHEN product_tier_name LIKE '%Premium%' THEN effective_start_date END) AS first_premium_date,
    MIN(CASE WHEN product_tier_name LIKE '%Ultimate%' THEN effective_start_date END) AS first_ultimate_date
  FROM mart_charge_filtered
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
  FROM mart_charge_filtered
  WHERE is_included_in_arr_calc = TRUE
    AND product_tier_name LIKE '%Ultimate%'
    AND product_category = 'Base Products'
    AND effective_start_date != effective_end_date
    AND arr > 0
    AND EXISTS (
      SELECT 1 FROM first_tier_dates
      WHERE first_tier_dates.dim_crm_account_id = mart_charge_filtered.dim_crm_account_id
        AND first_tier_dates.product_deployment_type = mart_charge_filtered.product_deployment_type
        AND first_tier_dates.first_ultimate_date = mart_charge_filtered.effective_start_date
    )
)

-- Capture initial Premium and Ultimate purchase events
, initial_purchase_events AS (
  SELECT 
    mart_charge_filtered.dim_crm_account_id,
    account_metadata.crm_account_name,
    mart_charge_filtered.dim_subscription_id_original,
    mart_charge_filtered.product_tier_name,
    mart_charge_filtered.product_deployment_type,
    DATE(mart_charge_filtered.effective_start_date) AS event_date,
    CASE 
      WHEN mart_charge_filtered.product_tier_name LIKE '%Premium%' THEN 'Premium Purchase'
      WHEN mart_charge_filtered.product_tier_name LIKE '%Ultimate%' THEN 'Ultimate Purchase'
    END AS stage_name,
    'subscription_purchase' AS source_type,
    mart_charge_filtered.product_deployment_type AS specific_deployment_type,
    mart_charge_filtered.arr AS subscription_arr,
    mart_charge_filtered.quantity AS license_quantity
  FROM mart_charge_filtered
  LEFT JOIN account_metadata
    ON mart_charge_filtered.dim_crm_account_id = account_metadata.dim_crm_account_id
  JOIN first_tier_dates
    ON mart_charge_filtered.dim_crm_account_id = first_tier_dates.dim_crm_account_id
    AND mart_charge_filtered.product_deployment_type = first_tier_dates.product_deployment_type
    AND (
      (mart_charge_filtered.product_tier_name LIKE '%Premium%' AND mart_charge_filtered.effective_start_date = first_tier_dates.first_premium_date) OR
      (mart_charge_filtered.product_tier_name LIKE '%Ultimate%' AND mart_charge_filtered.effective_start_date = first_tier_dates.first_ultimate_date)
    )
  WHERE mart_charge_filtered.is_included_in_arr_calc = TRUE
    AND (mart_charge_filtered.product_tier_name LIKE '%Premium%' OR mart_charge_filtered.product_tier_name LIKE '%Ultimate%')
    AND mart_charge_filtered.product_category = 'Base Products'
    AND mart_charge_filtered.effective_start_date != mart_charge_filtered.effective_end_date
    AND mart_charge_filtered.arr > 0
)

-- Capture all subscription events (latest version)
, all_subscription_events AS (
  SELECT 
    mart_charge_filtered.dim_crm_account_id,
    account_metadata.crm_account_name,
    mart_charge_filtered.dim_subscription_id_original,
    mart_charge_filtered.product_tier_name,
    mart_charge_filtered.product_deployment_type,
    DATE(mart_charge_filtered.effective_start_date) AS event_date,
    CASE 
      -- For Premium tier
      WHEN mart_charge_filtered.product_tier_name LIKE '%Premium%' AND EXISTS (
        SELECT 1 FROM first_tier_dates
        WHERE first_tier_dates.dim_crm_account_id = mart_charge_filtered.dim_crm_account_id
          AND first_tier_dates.product_deployment_type = mart_charge_filtered.product_deployment_type
          AND first_tier_dates.first_premium_date = mart_charge_filtered.effective_start_date
      ) THEN 'Premium Purchase'
      -- For Premium tier, but not the first one = expansion
      WHEN mart_charge_filtered.product_tier_name LIKE '%Premium%' THEN 'Premium Expansion'
      
      -- For Ultimate tier
      WHEN mart_charge_filtered.product_tier_name LIKE '%Ultimate%' AND EXISTS (
        SELECT 1 FROM first_tier_dates
        WHERE first_tier_dates.dim_crm_account_id = mart_charge_filtered.dim_crm_account_id
          AND first_tier_dates.product_deployment_type = mart_charge_filtered.product_deployment_type
          AND first_tier_dates.first_ultimate_date = mart_charge_filtered.effective_start_date
      ) THEN 'Ultimate Purchase'
      -- For Ultimate tier, but not the first one = expansion
      WHEN mart_charge_filtered.product_tier_name LIKE '%Ultimate%' THEN 'Ultimate Expansion'
      
      ELSE 'Unknown Purchase'
    END AS stage_name,
    CASE
      -- For first-time purchases
      WHEN (mart_charge_filtered.product_tier_name LIKE '%Premium%' OR mart_charge_filtered.product_tier_name LIKE '%Ultimate%') AND 
           EXISTS (
             SELECT 1 FROM first_tier_dates
             WHERE first_tier_dates.dim_crm_account_id = mart_charge_filtered.dim_crm_account_id
               AND first_tier_dates.product_deployment_type = mart_charge_filtered.product_deployment_type
               AND (
                 (mart_charge_filtered.product_tier_name LIKE '%Premium%' AND first_tier_dates.first_premium_date = mart_charge_filtered.effective_start_date) OR
                 (mart_charge_filtered.product_tier_name LIKE '%Ultimate%' AND first_tier_dates.first_ultimate_date = mart_charge_filtered.effective_start_date)
               )
           ) THEN 'subscription_purchase'
      -- For expansions
      ELSE 'license_expansion'
    END AS source_type,
    mart_charge_filtered.product_deployment_type AS specific_deployment_type,
    mart_charge_filtered.arr AS subscription_arr,
    mart_charge_filtered.quantity AS license_quantity
  FROM mart_charge_filtered
  JOIN latest_subscription_versions
    ON mart_charge_filtered.dim_subscription_id_original = latest_subscription_versions.dim_subscription_id_original
    AND mart_charge_filtered.effective_start_date = latest_subscription_versions.latest_effective_date
  LEFT JOIN account_metadata
    ON mart_charge_filtered.dim_crm_account_id = account_metadata.dim_crm_account_id
  WHERE mart_charge_filtered.is_included_in_arr_calc = TRUE
    AND (mart_charge_filtered.product_tier_name LIKE '%Premium%' OR mart_charge_filtered.product_tier_name LIKE '%Ultimate%')
    AND mart_charge_filtered.product_category = 'Base Products'
    AND mart_charge_filtered.effective_start_date != mart_charge_filtered.effective_end_date
    AND mart_charge_filtered.arr > 0
    -- Exclude records already captured in initial_purchase_events
    AND NOT (
      EXISTS (
        SELECT 1 FROM first_tier_dates
        WHERE first_tier_dates.dim_crm_account_id = mart_charge_filtered.dim_crm_account_id
          AND first_tier_dates.product_deployment_type = mart_charge_filtered.product_deployment_type
          AND (
            (mart_charge_filtered.product_tier_name LIKE '%Premium%' AND first_tier_dates.first_premium_date = mart_charge_filtered.effective_start_date) OR
            (mart_charge_filtered.product_tier_name LIKE '%Ultimate%' AND first_tier_dates.first_ultimate_date = mart_charge_filtered.effective_start_date)
          )
      )
    )
)

-- Capture all Premium ramp events
, all_premium_ramp_events AS (
  SELECT
    rpt_product_usage_health_score.dim_crm_account_id,
    account_metadata.crm_account_name,
    NULL AS dim_subscription_id_original,
    NULL AS product_tier_name,
    NULL AS product_deployment_type,
    DATE(rpt_product_usage_health_score.ping_created_at) AS event_date,
    'Premium Ramped' AS stage_name,
    'premium_usage_ramp' AS source_type,
    rpt_product_usage_health_score.deployment_type AS specific_deployment_type,
    NULL AS subscription_arr,
    NULL AS license_quantity
  FROM rpt_product_usage_health_score
  LEFT JOIN account_metadata
    ON rpt_product_usage_health_score.dim_crm_account_id = account_metadata.dim_crm_account_id
  WHERE rpt_product_usage_health_score.is_primary_instance_subscription = TRUE
    AND (
      rpt_product_usage_health_score.license_utilization_color = 'Green'
      OR rpt_product_usage_health_score.scm_color = 'Green'
      OR rpt_product_usage_health_score.ci_pipeline_utilization_color = 'Yellow' 
      OR rpt_product_usage_health_score.ci_pipeline_utilization_color = 'Green'
    )
    -- Join to ensure this is a Premium instance
    AND EXISTS (
      SELECT 1 
      FROM mart_arr
      WHERE mart_arr.arr_month = rpt_product_usage_health_score.snapshot_month
        AND mart_arr.product_deployment_type = rpt_product_usage_health_score.deployment_type
        AND mart_arr.dim_subscription_id_original = rpt_product_usage_health_score.dim_subscription_id_original
        AND mart_arr.product_tier_name ILIKE '%Premium%'
    )
)

-- Get latest subscription info for expansion analysis
, latest_subscription_info AS (
  SELECT
    dim_crm_account_id,
    dim_subscription_id_original,
    product_tier_name,
    product_deployment_type,
    effective_start_date,
    quantity,
    arr,
    LAG(quantity) OVER (
      PARTITION BY dim_crm_account_id, dim_subscription_id_original 
      ORDER BY effective_start_date
    ) AS previous_quantity,
    LAG(effective_start_date) OVER (
      PARTITION BY dim_crm_account_id, dim_subscription_id_original 
      ORDER BY effective_start_date
    ) AS previous_effective_date,
    LAG(arr) OVER (
      PARTITION BY dim_crm_account_id, dim_subscription_id_original 
      ORDER BY effective_start_date
    ) AS previous_arr,
    LAG(product_tier_name) OVER (
      PARTITION BY dim_crm_account_id, dim_subscription_id_original 
      ORDER BY effective_start_date
    ) AS previous_tier_name
  FROM mart_charge_filtered
  WHERE is_included_in_arr_calc = TRUE
    AND (product_tier_name LIKE '%Premium%' OR product_tier_name LIKE '%Ultimate%')
    AND product_category = 'Base Products'
    AND effective_start_date != effective_end_date
    AND quantity IS NOT NULL
)

-- Capture all Premium expansion events
, all_license_expansion_events AS (
  SELECT
    latest_subscription_info.dim_crm_account_id,
    account_metadata.crm_account_name,
    latest_subscription_info.dim_subscription_id_original,
    latest_subscription_info.product_tier_name,
    latest_subscription_info.product_deployment_type,
    DATE(latest_subscription_info.effective_start_date) AS event_date,
    CASE 
      WHEN latest_subscription_info.product_tier_name LIKE '%Premium%' THEN 'Premium Expansion'
      WHEN latest_subscription_info.product_tier_name LIKE '%Ultimate%' THEN 'Ultimate Expansion'
    END AS stage_name,
    'license_expansion' AS source_type,
    latest_subscription_info.product_deployment_type AS specific_deployment_type,
    latest_subscription_info.arr AS subscription_arr,
    latest_subscription_info.quantity AS license_quantity
  FROM latest_subscription_info
  LEFT JOIN account_metadata
    ON latest_subscription_info.dim_crm_account_id = account_metadata.dim_crm_account_id
  JOIN (
    -- Get the first Premium purchase date for each subscription
    SELECT
      dim_crm_account_id,
      dim_subscription_id_original,
      MIN(effective_start_date) AS first_premium_date
    FROM mart_charge_filtered
    WHERE product_tier_name LIKE '%Premium%'
      AND product_category = 'Base Products'
    GROUP BY dim_crm_account_id, dim_subscription_id_original
  ) first_premium ON latest_subscription_info.dim_crm_account_id = first_premium.dim_crm_account_id 
        AND latest_subscription_info.dim_subscription_id_original = first_premium.dim_subscription_id_original
  WHERE 
    -- Ensure this is a true expansion with increased quantity
    latest_subscription_info.quantity > COALESCE(latest_subscription_info.previous_quantity, 0)
    AND latest_subscription_info.effective_start_date > COALESCE(latest_subscription_info.previous_effective_date, '1900-01-01'::date)
    AND latest_subscription_info.quantity >= COALESCE(latest_subscription_info.previous_quantity, 0) * 1.10
    
    -- Handle tier-specific conditions
    AND (
      -- For Premium expansions
      (latest_subscription_info.product_tier_name LIKE '%Premium%' 
       AND DATEDIFF(day, first_premium.first_premium_date, latest_subscription_info.effective_start_date) >= 90)
       
      -- For Ultimate expansions, ensure it's not the first Ultimate
      OR (latest_subscription_info.product_tier_name LIKE '%Ultimate%'
          AND (latest_subscription_info.previous_tier_name LIKE '%Ultimate%'
               OR EXISTS (
                SELECT 1 
                FROM mart_charge_filtered earlier
                WHERE earlier.dim_crm_account_id = latest_subscription_info.dim_crm_account_id
                  AND earlier.product_tier_name LIKE '%Ultimate%'
                  AND earlier.product_deployment_type = latest_subscription_info.product_deployment_type
                  AND earlier.effective_start_date < latest_subscription_info.effective_start_date
                  AND earlier.is_included_in_arr_calc = TRUE
                  AND earlier.product_category = 'Base Products'
              )
          )
      )
    )
)

-- Capture all Ultimate consideration events
, all_ultimate_consideration_events AS (
  -- Trial events
  SELECT
    rpt_product_usage_marketing.dim_crm_account_id,
    account_metadata.crm_account_name,
    NULL AS dim_subscription_id_original,
    NULL AS product_tier_name,
    NULL AS product_deployment_type,
    DATE(rpt_product_usage_marketing.event_date) AS event_date,
    'Ultimate Consideration' AS stage_name,
    'ultimate_trial' AS source_type,
    NULL AS specific_deployment_type,
    NULL AS subscription_arr,
    NULL AS license_quantity
  FROM rpt_product_usage_marketing
  LEFT JOIN account_metadata
    ON rpt_product_usage_marketing.dim_crm_account_id = account_metadata.dim_crm_account_id
  WHERE rpt_product_usage_marketing.event_name = 'Trial' AND rpt_product_usage_marketing.trial_type LIKE '%Ultimate%'
  
  UNION ALL
  
  -- Security feature usage
  SELECT
    rpt_product_usage_health_score.dim_crm_account_id,
    account_metadata.crm_account_name,
    NULL AS dim_subscription_id_original,
    NULL AS product_tier_name,
    NULL AS product_deployment_type,
    DATE(rpt_product_usage_health_score.ping_created_at) AS event_date,
    'Ultimate Consideration' AS stage_name,
    'security_feature_usage' AS source_type,
    rpt_product_usage_health_score.deployment_type AS specific_deployment_type,
    NULL AS subscription_arr,
    NULL AS license_quantity
  FROM rpt_product_usage_health_score
  LEFT JOIN account_metadata
    ON rpt_product_usage_health_score.dim_crm_account_id = account_metadata.dim_crm_account_id
  WHERE rpt_product_usage_health_score.secure_scanners_utilization > 0
    AND rpt_product_usage_health_score.is_primary_instance_subscription = TRUE
    -- Only include if not on Ultimate yet (for consideration)
    AND NOT EXISTS (
      SELECT 1 
      FROM mart_arr
      WHERE mart_arr.arr_month = rpt_product_usage_health_score.snapshot_month
        AND mart_arr.product_deployment_type = rpt_product_usage_health_score.deployment_type
        AND mart_arr.dim_subscription_id_original = rpt_product_usage_health_score.dim_subscription_id_original
        AND mart_arr.product_tier_name ILIKE '%Ultimate%'
    )
  
  UNION ALL
  
  -- Ultimate opportunity creation
  SELECT
    mart_crm_opportunity.dim_crm_account_id,
    account_metadata.crm_account_name,
    NULL AS dim_subscription_id_original,
    NULL AS product_tier_name,
    NULL AS product_deployment_type,
    DATE(mart_crm_opportunity.created_date) AS event_date,
    'Ultimate Consideration' AS stage_name,
    'ultimate_opportunity' AS source_type,
    NULL AS specific_deployment_type,
    NULL AS subscription_arr,
    NULL AS license_quantity
  FROM mart_crm_opportunity
  LEFT JOIN account_metadata
    ON mart_crm_opportunity.dim_crm_account_id = account_metadata.dim_crm_account_id
  WHERE LOWER(mart_crm_opportunity.opportunity_name) LIKE '%ultimate%'
  
  UNION ALL
  
  -- Ultimate campaign touchpoints
  SELECT
    rpt_l2r_campaign_interactions_paid_account.dim_crm_account_id,
    account_metadata.crm_account_name,
    NULL AS dim_subscription_id_original,
    NULL AS product_tier_name,
    NULL AS product_deployment_type,
    DATE(rpt_l2r_campaign_interactions_paid_account.bizible_touchpoint_date) AS event_date,
    'Ultimate Consideration' AS stage_name,
    'ultimate_campaign' AS source_type,
    NULL AS specific_deployment_type,
    NULL AS subscription_arr,
    NULL AS license_quantity
  FROM rpt_l2r_campaign_interactions_paid_account
  LEFT JOIN account_metadata
    ON rpt_l2r_campaign_interactions_paid_account.dim_crm_account_id = account_metadata.dim_crm_account_id
  WHERE rpt_l2r_campaign_interactions_paid_account.touchpoint_type = 'Person Touchpoint'
    AND (rpt_l2r_campaign_interactions_paid_account.gtm_motion = 'Security & Compliance' OR rpt_l2r_campaign_interactions_paid_account.gtm_motion = 'DevSecOps Platform')
  
  UNION ALL
  
  -- Ultimate events
  SELECT
    mart_crm_event.dim_crm_account_id,
    account_metadata.crm_account_name,
    NULL AS dim_subscription_id_original,
    NULL AS product_tier_name,
    NULL AS product_deployment_type,
    DATE(mart_crm_event.created_at) AS event_date,
    'Ultimate Consideration' AS stage_name,
    'ultimate_event' AS source_type,
    NULL AS specific_deployment_type,
    NULL AS subscription_arr,
    NULL AS license_quantity
  FROM mart_crm_event
  LEFT JOIN account_metadata
    ON mart_crm_event.dim_crm_account_id = account_metadata.dim_crm_account_id
  WHERE (LOWER(mart_crm_event.event_subject) LIKE '%ultimate%' 
         OR LOWER(mart_crm_event.event_subject) LIKE '%security%'
         OR LOWER(mart_crm_event.event_description) LIKE '%ultimate%'
         OR LOWER(mart_crm_event.event_description) LIKE '%security%')
)

-- Capture all Ultimate onboarding & ramp events
, all_security_usage_events AS (
  -- Ultimate onboarding (started using security)
  SELECT
    rpt_product_usage_health_score.dim_crm_account_id,
    account_metadata.crm_account_name,
    NULL AS dim_subscription_id_original,
    NULL AS product_tier_name,
    NULL AS product_deployment_type,
    DATE(rpt_product_usage_health_score.ping_created_at) AS event_date,
    'Ultimate Onboarding' AS stage_name,
    'security_feature_usage' AS source_type,
    rpt_product_usage_health_score.deployment_type AS specific_deployment_type,
    NULL AS subscription_arr,
    NULL AS license_quantity
  FROM rpt_product_usage_health_score
  LEFT JOIN account_metadata
    ON rpt_product_usage_health_score.dim_crm_account_id = account_metadata.dim_crm_account_id
  WHERE rpt_product_usage_health_score.secure_scanners_utilization > 0
    AND rpt_product_usage_health_score.is_primary_instance_subscription = TRUE
    -- Only include if on Ultimate
    AND EXISTS (
      SELECT 1 
      FROM mart_arr
      WHERE mart_arr.arr_month = rpt_product_usage_health_score.snapshot_month
        AND mart_arr.product_deployment_type = rpt_product_usage_health_score.deployment_type
        AND mart_arr.dim_subscription_id_original = rpt_product_usage_health_score.dim_subscription_id_original
        AND mart_arr.product_tier_name ILIKE '%Ultimate%'
    )
  
  UNION ALL
  
  -- Ultimate ramped (security features reached Green)
  SELECT
    rpt_product_usage_health_score.dim_crm_account_id,
    account_metadata.crm_account_name,
    NULL AS dim_subscription_id_original,
    NULL AS product_tier_name,
    NULL AS product_deployment_type,
    DATE(rpt_product_usage_health_score.ping_created_at) AS event_date,
    'Ultimate Ramped' AS stage_name,
    'security_ramped' AS source_type,
    rpt_product_usage_health_score.deployment_type AS specific_deployment_type,
    NULL AS subscription_arr,
    NULL AS license_quantity
  FROM rpt_product_usage_health_score
  LEFT JOIN account_metadata
    ON rpt_product_usage_health_score.dim_crm_account_id = account_metadata.dim_crm_account_id
  WHERE rpt_product_usage_health_score.security_color_ultimate_only = 'Green'
    AND rpt_product_usage_health_score.is_primary_instance_subscription = TRUE
)

-- Capture all SA task activities
, all_sa_task_events AS (
  SELECT
    mart_crm_task.dim_crm_account_id,
    account_metadata.crm_account_name,
    NULL AS dim_subscription_id_original,
    NULL AS product_tier_name,
    NULL AS product_deployment_type,
    DATE(mart_crm_task.task_date) AS event_date,
    -- Categorize based on whether there's an Ultimate purchase before this date
    CASE 
      WHEN EXISTS (
        SELECT 1 
        FROM ultimate_purchase_dates 
        WHERE ultimate_purchase_dates.dim_crm_account_id = mart_crm_task.dim_crm_account_id
          AND ultimate_purchase_dates.ultimate_purchase_date <= DATE(mart_crm_task.task_date)
      ) THEN 'Ultimate Onboarding' 
      ELSE 'Ultimate Consideration'
    END AS stage_name,
    'sa_activity' AS source_type,
    NULL AS specific_deployment_type,
    NULL AS subscription_arr,
    NULL AS license_quantity
  FROM mart_crm_task
  LEFT JOIN account_metadata
    ON mart_crm_task.dim_crm_account_id = account_metadata.dim_crm_account_id
  WHERE mart_crm_task.sa_activity_type IS NOT NULL
)

-- Capture all demo task activities
, all_demo_task_events AS (
  SELECT
    mart_crm_task.dim_crm_account_id,
    account_metadata.crm_account_name,
    NULL AS dim_subscription_id_original,
    NULL AS product_tier_name,
    NULL AS product_deployment_type,
    DATE(COALESCE(mart_crm_task.task_completed_date, mart_crm_task.task_date)) AS event_date,
    -- Categorize based on whether there's an Ultimate purchase before this date
    CASE 
      WHEN EXISTS (
        SELECT 1 
        FROM ultimate_purchase_dates 
        WHERE ultimate_purchase_dates.dim_crm_account_id = mart_crm_task.dim_crm_account_id
          AND ultimate_purchase_dates.ultimate_purchase_date <= DATE(COALESCE(mart_crm_task.task_completed_date, mart_crm_task.task_date))
      ) THEN 'Ultimate Onboarding' 
      ELSE 'Ultimate Consideration'
    END AS stage_name,
    'demo_completion' AS source_type,
    NULL AS specific_deployment_type,
    NULL AS subscription_arr,
    NULL AS license_quantity
  FROM mart_crm_task
  LEFT JOIN account_metadata
    ON mart_crm_task.dim_crm_account_id = account_metadata.dim_crm_account_id
  WHERE mart_crm_task.is_demo_task = TRUE
    AND mart_crm_task.task_status = 'Completed'
    AND (mart_crm_task.task_completed_date IS NOT NULL OR mart_crm_task.task_date IS NOT NULL)
)

-- Combine all events
, all_journey_events AS (
  SELECT
    dim_crm_account_id,
    crm_account_name,
    stage_name,
    event_date,
    source_type,
    specific_deployment_type,
    subscription_arr,
    license_quantity
  FROM (
    SELECT * FROM initial_purchase_events
    UNION ALL SELECT * FROM all_subscription_events
    UNION ALL SELECT * FROM all_premium_ramp_events
    UNION ALL SELECT * FROM all_license_expansion_events
    UNION ALL SELECT * FROM all_ultimate_consideration_events
    UNION ALL SELECT * FROM all_security_usage_events
    UNION ALL SELECT * FROM all_sa_task_events
    UNION ALL SELECT * FROM all_demo_task_events
  )
  WHERE event_date IS NOT NULL
    AND event_date <= CURRENT_DATE()
)

-- Final output with all journey events
, final AS (
  SELECT
    all_journey_events.dim_crm_account_id,
    all_journey_events.crm_account_name,
    account_metadata.parent_crm_account_name,
    account_metadata.parent_crm_account_industry,
    account_metadata.parent_crm_account_sales_segment,
    account_metadata.parent_crm_account_geo AS parent_crm_account_geo_region,
    account_metadata.parent_crm_account_max_family_employee AS parent_crm_account_max_family_employee_count,
    all_journey_events.stage_name,
    all_journey_events.event_date,
    all_journey_events.source_type,
    all_journey_events.specific_deployment_type,
    all_journey_events.subscription_arr,
    all_journey_events.license_quantity
  FROM all_journey_events
  LEFT JOIN account_metadata
    ON all_journey_events.dim_crm_account_id = account_metadata.dim_crm_account_id
  -- Remove duplicate events on the same day for the same stage
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY all_journey_events.dim_crm_account_id, all_journey_events.event_date, all_journey_events.specific_deployment_type, all_journey_events.stage_name
    ORDER BY 
      -- Prioritize license_expansion over subscription_purchase when they happen on the same day
      CASE WHEN all_journey_events.source_type = 'license_expansion' THEN 1
           WHEN all_journey_events.source_type = 'subscription_purchase' THEN 2
           ELSE 3 END
  ) = 1
)

SELECT * 
FROM final
ORDER BY dim_crm_account_id, event_date