{{ config(
     materialized = "table"
) }}


{{ simple_cte([
    ('pte_scores', 'pte_scores_source'),
    ('ptc_scores', 'ptc_scores_source'),
    ('icp_successful_source', 'icp_successful_accounts_scores_source'),
    ('icp_paid_source', 'icp_paid_accounts_scores_source'),
    ('icp_lead_source', 'icp_lead_accounts_scores_source')
  ])
}}


, pte_latest_date AS (

    SELECT MAX(score_date) AS latest_score_date
    FROM pte_scores

), ptc_latest_date AS (

    SELECT MAX(score_date) AS latest_score_date
    FROM ptc_scores

), icp_successful_latest_date AS (

    SELECT MAX(score_date) AS latest_score_date
    FROM icp_successful_source
                 
), icp_paid_latest_date AS (

    SELECT MAX(score_date) AS latest_score_date
    FROM icp_paid_source

), icp_lead_latest_date AS (

    SELECT MAX(score_date) AS latest_score_date
    FROM icp_lead_source

), pte AS (

    SELECT
      crm_account_id,
      score_date AS pte_score_date,
      score AS pte_score,
      score_group AS pte_score_group,
      (score - AVG(score) OVER (PARTITION BY score_date)) / STDDEV(score) OVER (PARTITION BY score_date) AS pte_distance, -- (SCORE / AVERAGE SCORE FOR THAT DATE) / (STDEV FOR THAT DATE)
      insights AS pte_insights,
      uptier_likely AS pte_uptier_likely,
    FROM pte_scores
    WHERE score_date = (SELECT latest_score_date FROM pte_latest_date)

), ptc AS (

    SELECT
      crm_account_id,
      score_date AS ptc_score_date,
      score AS ptc_score,
      score_group AS ptc_score_group,
      (score - AVG(score) OVER (PARTITION BY score_date)) / STDDEV(score) OVER (PARTITION BY score_date) AS ptc_distance, -- (SCORE / AVERAGE SCORE FOR THAT DATE) / (STDEV FOR THAT DATE)
      insights AS ptc_insights,
      downtier_likely AS ptc_downtier_likely,
      renewal_date
    FROM ptc_scores
    WHERE score_date = (SELECT latest_score_date FROM ptc_latest_date)

), icp_successful AS (

    SELECT
      crm_account_id,
      score_date AS icp_score_date,
      CONCAT(round(cluster_id,0), ' - ', cluster_name) AS icp_name
    FROM icp_successful_source
    WHERE cluster_id != 0
      AND score_date = (SELECT latest_score_date FROM icp_successful_latest_date)

), icp_paid AS (

    SELECT 
        crm_account_id,
        score_date as icp_score_date,
        CASE WHEN predicted_cluster_id IN (1,1.1) 
          THEN 'DevSecOps-Minded Enterprises'
          ELSE predicted_cluster_name END AS predicted_cluster_name_simple,
        CONCAT('PREDICTED: ', round(predicted_cluster_id,0), ' - ', predicted_cluster_name_simple) AS icp_name,
    FROM icp_paid_source
    WHERE predicted_cluster_id != 0
      AND successful_customer_cluster_id IS NULL
      AND score_date = (SELECT latest_score_date FROM icp_paid_latest_date)

), icp_lead AS (

    SELECT 
        crm_account_id,
        score_date as icp_score_date,
        CONCAT('PREDICTED: ', round(predicted_cluster_id,0), ' - ', predicted_cluster_name) AS icp_name,
    FROM icp_lead_source
    WHERE predicted_cluster_id != 0
      AND score_date = (SELECT latest_score_date FROM icp_lead_latest_date)

), all_accounts AS (
-- Get all distinct account IDs to avoid having dupes in the final table
    SELECT crm_account_id 
    FROM (
        SELECT crm_account_id FROM pte
        UNION
        SELECT crm_account_id FROM ptc
        UNION
        SELECT crm_account_id FROM icp_successful
        UNION
        SELECT crm_account_id FROM icp_paid
        UNION
        SELECT crm_account_id FROM icp_lead
    )
)

SELECT
  base.crm_account_id,
  a.pte_score_date,
  a.pte_score,
  CASE
    WHEN a.pte_score_group = 5 AND b.ptc_score_group = 1 AND a.pte_distance < b.ptc_distance
      THEN 4
    ELSE a.pte_score_group
  END AS pte_stars,
  a.pte_insights,
  a.pte_uptier_likely,
  b.ptc_score_date,
  b.ptc_score,
  CASE
    WHEN a.pte_score_group = 5 AND b.ptc_score_group = 1 AND a.pte_distance >= b.ptc_distance
      THEN 2
    ELSE b.ptc_score_group
  END AS ptc_stars,
  b.ptc_insights,
  b.ptc_downtier_likely,
  b.renewal_date,
  COALESCE(c.icp_score_date, d.icp_score_date, e.icp_score_date) AS icp_score_date,
  COALESCE(c.icp_name, d.icp_name, e.icp_name) AS icp_name,
  CASE WHEN c.crm_account_id IS NOT NULL THEN TRUE
       ELSE FALSE
       END AS icp_meets_success_criteria_flag
 FROM all_accounts base
 LEFT JOIN pte a
    ON base.crm_account_id = a.crm_account_id
 LEFT JOIN ptc b
    ON base.crm_account_id = b.crm_account_id
 LEFT JOIN icp_successful c
    ON base.crm_account_id = c.crm_account_id
 LEFT JOIN icp_paid d
    ON base.crm_account_id = d.crm_account_id
 LEFT JOIN icp_lead e
    ON base.crm_account_id = e.crm_account_id
