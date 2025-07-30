{{ config(
    materialized="table"
) }}

{{ simple_cte([
    ('prep_namespace_order_trial','prep_namespace_order_trial'),
    ('fct_trial','fct_trial'),
    ('mart_crm_event', 'mart_crm_event'),
    ('mart_crm_task','mart_crm_task'),
    ('map_namespace_subscription_product','map_namespace_subscription_product'),
    ('rpt_lead_to_revenue', 'rpt_lead_to_revenue'),
    ('mart_crm_account','mart_crm_account')
]) }}

, mart_crm_activity AS (

  SELECT
        dim_crm_task_pk,
        task_id,
        dim_crm_account_id,
        task_date,
    FROM mart_crm_task
    WHERE task_date >= '2024-02-01'
        AND task_type IN ('Business Plan','(ii) Discovery call','Call','Demo','Email','Mail','Meeting','sequence_step_linkedin_send_message')
    
    UNION ALL
    
    SELECT
        dim_crm_event_pk,
        event_id,
        dim_crm_account_id,
        event_date
    FROM mart_crm_event
    WHERE event_date >= '2024-02-01'
        AND (CONTAINS(sa_activity_type, 'POV') 
          OR CONTAINS(sa_activity_type, 'Discovery session')
          OR CONTAINS(sa_activity_type, 'Customer Strategy Plan Review')
          OR CONTAINS(sa_activity_type, 'Executive Solution Plan')
          OR CONTAINS(sa_activity_type, 'Demo')
          OR CONTAINS(sa_activity_type, 'Guided-Trial')
          OR CONTAINS(sa_activity_type, 'Presentation / pitch')
          OR (
              CONTAINS(sa_activity_type, 'Technical Deep Dive')
              AND NOT CONTAINS(sa_activity_type, 'account')
              AND NOT CONTAINS(sa_activity_type, 'support')
          )
        )

), 
trial_base AS (

    SELECT DISTINCT
        prep_namespace_order_trial.dim_namespace_id,
        mart_crm_activity.dim_crm_account_id,
        prep_namespace_order_trial.trial_type,
        prep_namespace_order_trial.trial_type_name,
        fct_trial.trial_start_date,
        fct_trial.trial_end_date,
        CASE
            WHEN mart_crm_activity.task_date >= trial_start_date
                AND mart_crm_activity.task_date <= trial_end_date
                AND trial_start_date IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS is_engaged_during_trial_prep,
        ARRAY_UNIQUE_AGG(fct_trial.product_rate_plan_id) AS product_rate_plan_id_array
    FROM prep_namespace_order_trial
    LEFT JOIN fct_trial
        ON prep_namespace_order_trial.dim_namespace_id = fct_trial.dim_namespace_id
            AND prep_namespace_order_trial.order_start_date = fct_trial.trial_start_date
    LEFT JOIN map_namespace_subscription_product
        ON prep_namespace_order_trial.dim_namespace_id=map_namespace_subscription_product.dim_namespace_id
    LEFT JOIN mart_crm_activity
        ON map_namespace_subscription_product.dim_crm_account_id=mart_crm_activity.dim_crm_account_id
    WHERE fct_trial.trial_start_date >= '2024-02-01'
      OR fct_trial.trial_start_date IS NULL
    {{dbt_utils.group_by(n=7)}}

), trial_final AS (

    SELECT
        trial_base.dim_namespace_id,
        trial_base.dim_crm_account_id,
        trial_base.trial_type,
        trial_base.trial_type_name,
        trial_base.trial_start_date,
        trial_base.trial_end_date,
        trial_base.product_rate_plan_id_array,
        CASE
        WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'free-plan-id')
          THEN CASE
              WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'ultimate-saas-trial-plan-id')
              THEN 'Free Ultimate SaaS'
              WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'saas-gitlab-duo-pro-trial-plan-id')
              THEN 'Free Duo Pro'
              WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'saas-gitlab-duo-enterprise-trial-plan-id')
              THEN 'Free Duo Enterprise'
              WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'premium-saas-trial-plan-id')
              THEN 'Free SaaS'
              WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'ultimate-saas-trial-w-duo-enterprise-trial-plan-id')
              THEN 'Free SaaS With Duo'
              ELSE 'Free Something'
              END
          WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'ultimate-saas-trial-plan-id')
            THEN 'SaaS Ultimate'
          WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'saas-gitlab-duo-pro-trial-plan-id')
            THEN 'SaaS Duo'
          WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'saas-gitlab-duo-enterprise-trial-plan-id')
            THEN 'SaaS Duo Enterprise'
          WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'premium-saas-trial-plan-id')
            THEN 'SaaS Premium'
          WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'ultimate-saas-trial-paid-customer-w-duo-enterprise-trial-plan-id')
            THEN 'SaaS Ultimate with Duo Enterprise Paid'
          WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'ultimate-saas-trial-paid-customer-plan-id')
            THEN 'SaaS Ultimate Paid'
          WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'ultimate-saas-trial-w-duo-enterprise-trial-plan-id')
            THEN 'SaaS Ultimate with Duo Enterprise'
          WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), '2c92a0fe6145beb901614f137a0f1685')
            THEN 'Ultimate Self-Managed'
          WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), '2c92a0ff6145d07001614f0ca2796525')
            THEN 'Premium Self-Managed'
          WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), '2c92a0ff76f0d5250176f2f8c86f305a')
            THEN 'Ultimate SaaS'
          WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), '2c92a00d76f0d5060176f2fb0a5029ff')
            THEN 'Premium SaaS'
          WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), '2c92a0fc5a83f01d015aa6db83c45aac')
            THEN 'Gold Plan - aka Ultimate for GitLab.com'
          WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), '2c92a0fd5a840403015aa6d9ea2c46d6')
            THEN 'Silver Plan - aka Premium for GitLab.com'
          WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), '2c92a0ff5a840412015aa3cde86f2ba6')
            THEN 'Bronze Plan - aka Starter for GitLab.com'
          ELSE 'Not Grouped'
        END AS trial_type_2,
        MAX(trial_base.is_engaged_during_trial_prep) AS is_engaged_during_trial
    FROM trial_base 
    {{dbt_utils.group_by(n=8)}}

), task_base AS (

    SELECT DISTINCT
        dim_crm_account_id,
        COUNT(DISTINCT task_id) AS sa_task_count,
        MAX(task_date)::DATE AS last_account_task_date
    FROM mart_crm_task
    WHERE sa_activity_type IS NOT NULL
      AND task_date >= '2024-02-01'
    GROUP BY 1

)

    SELECT
        CONCAT('https://gitlab.lightning.force.com/lightning/r/Account/',rpt_lead_to_revenue.dim_crm_account_id,'/view') AS sfdc_account_url,
        rpt_lead_to_revenue.dim_crm_account_id,
        rpt_lead_to_revenue.dim_crm_opportunity_id,
        mart_crm_account.crm_account_owner,
        mart_crm_account.crm_account_owner_area,
        mart_crm_account.crm_account_owner_geo,
        mart_crm_account.crm_account_owner_region,
        mart_crm_account.crm_account_owner_user_segment,
        trial_final.trial_type,
        trial_final.trial_type_2 AS trial_type_grouped,
        trial_final.trial_type_name,
        trial_final.trial_start_date,
        trial_final.trial_end_date,
        trial_final.is_engaged_during_trial,
        task_base.sa_task_count,
        task_base.last_account_task_date,
        CASE
            WHEN trial_final.trial_start_date IS NULL
            THEN TRUE
            ELSE FALSE
        END AS has_not_started_trial,
        CASE
            WHEN trial_final.trial_start_date <= CURRENT_DATE
                AND trial_final.trial_end_date > CURRENT_DATE
            THEN TRUE
            ELSE FALSE
        END AS is_still_in_trial,
        CASE
            WHEN trial_final.trial_end_date <= CURRENT_DATE
              AND trial_final.trial_start_date IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS is_finished_trial
    FROM rpt_lead_to_revenue
    LEFT JOIN trial_final
        ON rpt_lead_to_revenue.dim_crm_account_id=trial_final.dim_crm_account_id
    LEFT JOIN task_base
        ON rpt_lead_to_revenue.dim_crm_account_id=task_base.dim_crm_account_id
    LEFT JOIN mart_crm_account
        ON rpt_lead_to_revenue.dim_crm_account_id=mart_crm_account.dim_crm_account_id