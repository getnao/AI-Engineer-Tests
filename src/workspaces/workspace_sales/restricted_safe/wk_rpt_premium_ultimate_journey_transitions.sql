{{
  config(
    materialized='table'
  )
}}

{{ simple_cte([
    ('wk_rpt_premium_ultimate_journey_current_state', 'wk_rpt_premium_ultimate_journey_current_state')
]) }}

-- Create milestone events from all relevant dates with triggering activities
, milestone_events AS (
  SELECT 
    dim_crm_account_id,
    crm_account_name,
    'Premium Purchase' AS stage_name,
    first_premium_date AS event_date,
    1 AS stage_sequence,
    'Subscription Start' AS trigger_activity,
    'Premium subscription became active' AS trigger_detail
  FROM wk_rpt_premium_ultimate_journey_current_state
  WHERE first_premium_date IS NOT NULL
  
  UNION ALL
  
  SELECT 
    dim_crm_account_id,
    crm_account_name,
    'Premium Ramped' AS stage_name,
    first_premium_ramped_date AS event_date,
    2 AS stage_sequence,
    'Product Usage Milestone' AS trigger_activity,
    'Achieved Premium feature adoption (License/SCM/CI utilization)' AS trigger_detail
  FROM wk_rpt_premium_ultimate_journey_current_state
  WHERE first_premium_ramped_date IS NOT NULL
  
  UNION ALL
  
  SELECT 
    dim_crm_account_id,
    crm_account_name,
    'Premium Expansion' AS stage_name,
    first_premium_expansion_date AS event_date,
    3 AS stage_sequence,
    'License Expansion' AS trigger_activity,
    'Premium license quantity increased by 10%+' AS trigger_detail
  FROM wk_rpt_premium_ultimate_journey_current_state
  WHERE first_premium_expansion_date IS NOT NULL
    AND DATEDIFF(day, first_premium_date, first_premium_expansion_date) >= 90
  
  UNION ALL
  
  SELECT 
    dim_crm_account_id,
    crm_account_name,
    'Ultimate Consideration' AS stage_name,
    -- Take earliest consideration indicator
    LEAST(
      COALESCE(first_ultimate_trial_date, '9999-12-31'),
      COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31'),
      COALESCE(first_ultimate_opportunity_created_date, '9999-12-31'),
      COALESCE(first_ultimate_campaign_date, '9999-12-31'),
      COALESCE(first_ultimate_event_created_date, '9999-12-31'),
      COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31'),
      COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31')
    ) AS event_date,
    4 AS stage_sequence,
    -- Determine which activity triggered the consideration stage
    CASE 
      WHEN COALESCE(first_ultimate_trial_date, '9999-12-31') = LEAST(
        COALESCE(first_ultimate_trial_date, '9999-12-31'),
        COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(first_ultimate_opportunity_created_date, '9999-12-31'),
        COALESCE(first_ultimate_campaign_date, '9999-12-31'),
        COALESCE(first_ultimate_event_created_date, '9999-12-31'),
        COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Ultimate Trial'
      WHEN COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31') = LEAST(
        COALESCE(first_ultimate_trial_date, '9999-12-31'),
        COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(first_ultimate_opportunity_created_date, '9999-12-31'),
        COALESCE(first_ultimate_campaign_date, '9999-12-31'),
        COALESCE(first_ultimate_event_created_date, '9999-12-31'),
        COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Security Feature Exploration'
      WHEN COALESCE(first_ultimate_opportunity_created_date, '9999-12-31') = LEAST(
        COALESCE(first_ultimate_trial_date, '9999-12-31'),
        COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(first_ultimate_opportunity_created_date, '9999-12-31'),
        COALESCE(first_ultimate_campaign_date, '9999-12-31'),
        COALESCE(first_ultimate_event_created_date, '9999-12-31'),
        COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Sales Opportunity'
      WHEN COALESCE(first_ultimate_campaign_date, '9999-12-31') = LEAST(
        COALESCE(first_ultimate_trial_date, '9999-12-31'),
        COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(first_ultimate_opportunity_created_date, '9999-12-31'),
        COALESCE(first_ultimate_campaign_date, '9999-12-31'),
        COALESCE(first_ultimate_event_created_date, '9999-12-31'),
        COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Marketing Campaign'
      WHEN COALESCE(first_ultimate_event_created_date, '9999-12-31') = LEAST(
        COALESCE(first_ultimate_trial_date, '9999-12-31'),
        COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(first_ultimate_opportunity_created_date, '9999-12-31'),
        COALESCE(first_ultimate_campaign_date, '9999-12-31'),
        COALESCE(first_ultimate_event_created_date, '9999-12-31'),
        COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Sales Event'
      WHEN COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31') = LEAST(
        COALESCE(first_ultimate_trial_date, '9999-12-31'),
        COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(first_ultimate_opportunity_created_date, '9999-12-31'),
        COALESCE(first_ultimate_campaign_date, '9999-12-31'),
        COALESCE(first_ultimate_event_created_date, '9999-12-31'),
        COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Solution Architect Activity'
      WHEN COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31') = LEAST(
        COALESCE(first_ultimate_trial_date, '9999-12-31'),
        COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(first_ultimate_opportunity_created_date, '9999-12-31'),
        COALESCE(first_ultimate_campaign_date, '9999-12-31'),
        COALESCE(first_ultimate_event_created_date, '9999-12-31'),
        COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Demo Completed'
      ELSE 'Multiple Activities'
    END AS trigger_activity,
    CASE 
      WHEN COALESCE(first_ultimate_trial_date, '9999-12-31') = LEAST(
        COALESCE(first_ultimate_trial_date, '9999-12-31'),
        COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(first_ultimate_opportunity_created_date, '9999-12-31'),
        COALESCE(first_ultimate_campaign_date, '9999-12-31'),
        COALESCE(first_ultimate_event_created_date, '9999-12-31'),
        COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Started Ultimate trial before purchase'
      WHEN COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31') = LEAST(
        COALESCE(first_ultimate_trial_date, '9999-12-31'),
        COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(first_ultimate_opportunity_created_date, '9999-12-31'),
        COALESCE(first_ultimate_campaign_date, '9999-12-31'),
        COALESCE(first_ultimate_event_created_date, '9999-12-31'),
        COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Used security features before Ultimate purchase'
      WHEN COALESCE(first_ultimate_opportunity_created_date, '9999-12-31') = LEAST(
        COALESCE(first_ultimate_trial_date, '9999-12-31'),
        COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(first_ultimate_opportunity_created_date, '9999-12-31'),
        COALESCE(first_ultimate_campaign_date, '9999-12-31'),
        COALESCE(first_ultimate_event_created_date, '9999-12-31'),
        COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Ultimate sales opportunity created'
      WHEN COALESCE(first_ultimate_campaign_date, '9999-12-31') = LEAST(
        COALESCE(first_ultimate_trial_date, '9999-12-31'),
        COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(first_ultimate_opportunity_created_date, '9999-12-31'),
        COALESCE(first_ultimate_campaign_date, '9999-12-31'),
        COALESCE(first_ultimate_event_created_date, '9999-12-31'),
        COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31')
      ) THEN CONCAT('Security/DevSecOps campaign interaction: ', COALESCE(ultimate_campaign_motions, 'Security & Compliance'))
      WHEN COALESCE(first_ultimate_event_created_date, '9999-12-31') = LEAST(
        COALESCE(first_ultimate_trial_date, '9999-12-31'),
        COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(first_ultimate_opportunity_created_date, '9999-12-31'),
        COALESCE(first_ultimate_campaign_date, '9999-12-31'),
        COALESCE(first_ultimate_event_created_date, '9999-12-31'),
        COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Sales event with Ultimate/security keywords'
      WHEN COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31') = LEAST(
        COALESCE(first_ultimate_trial_date, '9999-12-31'),
        COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(first_ultimate_opportunity_created_date, '9999-12-31'),
        COALESCE(first_ultimate_campaign_date, '9999-12-31'),
        COALESCE(first_ultimate_event_created_date, '9999-12-31'),
        COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Solution Architect engagement before Ultimate purchase'
      WHEN COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31') = LEAST(
        COALESCE(first_ultimate_trial_date, '9999-12-31'),
        COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(first_ultimate_opportunity_created_date, '9999-12-31'),
        COALESCE(first_ultimate_campaign_date, '9999-12-31'),
        COALESCE(first_ultimate_event_created_date, '9999-12-31'),
        COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Product demo completed before Ultimate purchase'
      ELSE 'Multiple consideration activities on same date'
    END AS trigger_detail
  FROM wk_rpt_premium_ultimate_journey_current_state
  WHERE (
    first_ultimate_trial_date IS NOT NULL
    OR security_feature_before_ultimate_purchase_date IS NOT NULL
    OR first_ultimate_opportunity_created_date IS NOT NULL
    OR first_ultimate_campaign_date IS NOT NULL
    OR first_ultimate_event_created_date IS NOT NULL
    OR sa_activity_before_ultimate_purchase_date IS NOT NULL
    OR demo_completed_before_ultimate_purchase_date IS NOT NULL
  )
  -- CRITICAL: Only include if the Ultimate Consideration happens BEFORE Ultimate Purchase
  AND (
    first_ultimate_date IS NULL 
    OR LEAST(
      COALESCE(first_ultimate_trial_date, '9999-12-31'),
      COALESCE(security_feature_before_ultimate_purchase_date, '9999-12-31'),
      COALESCE(first_ultimate_opportunity_created_date, '9999-12-31'),
      COALESCE(first_ultimate_campaign_date, '9999-12-31'),
      COALESCE(first_ultimate_event_created_date, '9999-12-31'),
      COALESCE(sa_activity_before_ultimate_purchase_date, '9999-12-31'),
      COALESCE(demo_completed_before_ultimate_purchase_date, '9999-12-31')
    ) < first_ultimate_date
  )
  
  UNION ALL
  
  SELECT 
    dim_crm_account_id,
    crm_account_name,
    'Ultimate Purchase' AS stage_name,
    first_ultimate_date AS event_date,
    5 AS stage_sequence,
    'Subscription Upgrade' AS trigger_activity,
    'Ultimate subscription became active' AS trigger_detail
  FROM wk_rpt_premium_ultimate_journey_current_state
  WHERE first_ultimate_date IS NOT NULL
  
  UNION ALL
  
  SELECT 
    dim_crm_account_id,
    crm_account_name,
    'Ultimate Onboarding' AS stage_name,
    -- Using earliest of security feature usage or SA/demo onboarding activities AFTER Ultimate Purchase
    CASE
      WHEN first_ultimate_date IS NOT NULL THEN
        LEAST(
          COALESCE(security_feature_after_ultimate_purchase_date, '9999-12-31'),
          COALESCE(sa_activity_after_ultimate_purchase_date, '9999-12-31'),
          COALESCE(demo_completed_after_ultimate_purchase_date, '9999-12-31')
        )
      ELSE NULL
    END AS event_date,
    6 AS stage_sequence,
    -- Determine which activity triggered the onboarding stage
    CASE 
      WHEN COALESCE(security_feature_after_ultimate_purchase_date, '9999-12-31') = LEAST(
        COALESCE(security_feature_after_ultimate_purchase_date, '9999-12-31'),
        COALESCE(sa_activity_after_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_after_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Security Feature Usage'
      WHEN COALESCE(sa_activity_after_ultimate_purchase_date, '9999-12-31') = LEAST(
        COALESCE(security_feature_after_ultimate_purchase_date, '9999-12-31'),
        COALESCE(sa_activity_after_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_after_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Solution Architect Activity'
      WHEN COALESCE(demo_completed_after_ultimate_purchase_date, '9999-12-31') = LEAST(
        COALESCE(security_feature_after_ultimate_purchase_date, '9999-12-31'),
        COALESCE(sa_activity_after_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_after_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Demo Completed'
      ELSE 'Multiple Activities'
    END AS trigger_activity,
    CASE 
      WHEN COALESCE(security_feature_after_ultimate_purchase_date, '9999-12-31') = LEAST(
        COALESCE(security_feature_after_ultimate_purchase_date, '9999-12-31'),
        COALESCE(sa_activity_after_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_after_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Started using security features after Ultimate purchase'
      WHEN COALESCE(sa_activity_after_ultimate_purchase_date, '9999-12-31') = LEAST(
        COALESCE(security_feature_after_ultimate_purchase_date, '9999-12-31'),
        COALESCE(sa_activity_after_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_after_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Solution Architect engagement for Ultimate onboarding'
      WHEN COALESCE(demo_completed_after_ultimate_purchase_date, '9999-12-31') = LEAST(
        COALESCE(security_feature_after_ultimate_purchase_date, '9999-12-31'),
        COALESCE(sa_activity_after_ultimate_purchase_date, '9999-12-31'),
        COALESCE(demo_completed_after_ultimate_purchase_date, '9999-12-31')
      ) THEN 'Product demo completed after Ultimate purchase'
      ELSE 'Multiple onboarding activities on same date'
    END AS trigger_detail
  FROM wk_rpt_premium_ultimate_journey_current_state
  WHERE first_ultimate_date IS NOT NULL
    AND (
      security_feature_after_ultimate_purchase_date IS NOT NULL
      OR sa_activity_after_ultimate_purchase_date IS NOT NULL
      OR demo_completed_after_ultimate_purchase_date IS NOT NULL
    )
    AND security_ramped_after_ultimate_purchase_date IS NULL -- Only include if they haven't reached ramped yet
  
  UNION ALL
  
  SELECT 
    dim_crm_account_id,
    crm_account_name,
    'Ultimate Expansion' AS stage_name,
    first_ultimate_expansion_date AS event_date,
    7 AS stage_sequence,
    'License Expansion' AS trigger_activity,
    'Ultimate license quantity increased by 10%+' AS trigger_detail
  FROM wk_rpt_premium_ultimate_journey_current_state
  WHERE first_ultimate_expansion_date IS NOT NULL
    -- Ensure this only happens after Ultimate Purchase and has minimum time gap
    AND first_ultimate_date IS NOT NULL
    AND first_ultimate_expansion_date > first_ultimate_date
    AND DATEDIFF(day, first_ultimate_date, first_ultimate_expansion_date) >= 90
  
  UNION ALL
  
  SELECT 
    dim_crm_account_id,
    crm_account_name,
    'Ultimate Ramped' AS stage_name,
    security_ramped_after_ultimate_purchase_date AS event_date,
    8 AS stage_sequence,
    'Security Features Adoption' AS trigger_activity,
    'Achieved Green security health score (ramped on security features)' AS trigger_detail
  FROM wk_rpt_premium_ultimate_journey_current_state
  WHERE security_ramped_after_ultimate_purchase_date IS NOT NULL
    -- Ensure this only happens after Ultimate Purchase
    AND first_ultimate_date IS NOT NULL
    AND security_ramped_after_ultimate_purchase_date > first_ultimate_date
)

-- Rank stages by date for each account to handle multiple stages on same date
, ranked_stages AS (
  SELECT
    dim_crm_account_id,
    crm_account_name,
    stage_name,
    event_date,
    stage_sequence,
    trigger_activity,
    trigger_detail,
    ROW_NUMBER() OVER (
      PARTITION BY dim_crm_account_id 
      ORDER BY event_date, stage_sequence
    ) AS event_order
  FROM milestone_events
  WHERE event_date IS NOT NULL
)

-- Create stage history with entry and exit dates
, stage_history AS (
  SELECT
    ranked_stages.dim_crm_account_id,
    ranked_stages.crm_account_name,
    ranked_stages.stage_name,
    ranked_stages.event_date AS entry_date,
    ranked_stages.trigger_activity,
    ranked_stages.trigger_detail,
    CASE
      -- Set exit_date to NULL when it's the latest stage for the account
      WHEN LEAD(ranked_stages.event_date) OVER (
        PARTITION BY ranked_stages.dim_crm_account_id 
        ORDER BY ranked_stages.event_order
      ) IS NULL THEN NULL
      -- Otherwise use the next stage's entry date
      ELSE LEAD(ranked_stages.event_date) OVER (
        PARTITION BY ranked_stages.dim_crm_account_id 
        ORDER BY ranked_stages.event_order
      )
    END AS exit_date,
    ranked_stages.stage_sequence,
    ranked_stages.event_order,
    LEAD(ranked_stages.stage_name) OVER (
      PARTITION BY ranked_stages.dim_crm_account_id 
      ORDER BY ranked_stages.event_order
    ) AS next_stage,
    wk_rpt_premium_ultimate_journey_current_state.journey_stage AS current_journey_stage,
    wk_rpt_premium_ultimate_journey_current_state.current_tier,
    (LEAD(ranked_stages.event_date) OVER (
      PARTITION BY ranked_stages.dim_crm_account_id 
      ORDER BY ranked_stages.event_order
    ) IS NULL) AS is_current_stage,
    wk_rpt_premium_ultimate_journey_current_state.deployment_types,
    wk_rpt_premium_ultimate_journey_current_state.deployment_type_count
  FROM ranked_stages
  JOIN wk_rpt_premium_ultimate_journey_current_state 
    ON ranked_stages.dim_crm_account_id = wk_rpt_premium_ultimate_journey_current_state.dim_crm_account_id
)

-- Final output with days in stage calculation
, final AS (
  SELECT
    dim_crm_account_id,
    crm_account_name,
    stage_name,
    entry_date,
    exit_date,
    trigger_activity,
    trigger_detail,
    CASE
      -- Calculate days_in_stage for completed stages
      WHEN exit_date IS NOT NULL THEN DATEDIFF(day, entry_date, exit_date)
      -- For current stage, calculate days from entry_date to current_date
      ELSE DATEDIFF(day, entry_date, CURRENT_DATE())
    END AS days_in_stage,
    stage_sequence,
    next_stage,
    current_journey_stage,
    current_tier,
    is_current_stage,
    deployment_types,
    deployment_type_count
  FROM stage_history
)

SELECT * 
FROM final
ORDER BY dim_crm_account_id, entry_date, stage_sequence