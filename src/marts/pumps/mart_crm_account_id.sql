WITH all_accounts AS (

  SELECT DISTINCT crm_account_id
  FROM {{ ref('pte_scores_source') }}

  UNION

  SELECT DISTINCT crm_account_id
  FROM {{ ref('ptc_scores_source') }}

  UNION

  SELECT DISTINCT crm_account_id
  FROM {{ ref('icp_successful_accounts_scores_source') }}
  WHERE cluster_id != 0
        
  UNION
        
  SELECT DISTINCT crm_account_id,
  FROM {{ ref('icp_paid_accounts_scores_source') }}
  WHERE predicted_cluster_id != 0
    AND successful_customer_cluster_id IS NULL

  UNION

  SELECT DISTINCT crm_account_id,
  FROM {{ ref('icp_lead_accounts_scores_source') }}
  WHERE predicted_cluster_id != 0

),

pte_scores AS (
  SELECT
    crm_account_id,
    pte_score,
    pte_stars,
    pte_insights,
    pte_uptier_likely
  FROM {{ ref('model_mart_crm_account_id') }}
  WHERE pte_score_date IS NOT NULL 

),

ptc_scores AS (
  SELECT
    crm_account_id,
    ptc_score,
    ptc_stars,
    ptc_insights,
    ptc_downtier_likely
  FROM {{ ref('model_mart_crm_account_id') }}
  WHERE ptc_score_date IS NOT NULL 

),

icp_scores AS (
  SELECT
    crm_account_id,
    icp_name,
    icp_meets_success_criteria_flag,
    icp_score_date
  FROM {{ ref('model_mart_crm_account_id') }}
  WHERE icp_score_date IS NOT NULL  

)

SELECT
  all_accounts.crm_account_id AS id,
  pte_stars AS pte_score_value__c,
  pte_insights AS pte_insights__c,
  pte_uptier_likely AS pte_likely_to_uptier__c,
  ptc_downtier_likely AS ptc_downtier_likely__c,
  ptc_stars AS ptc_score_value__c,
  ptc_insights AS ptc_insights__c,
  pte_score * 100 AS pte_percent__c,
  ptc_score * 100 AS ptc_percent__c,
  icp_name AS icp_profile_name__c,
  icp_meets_success_criteria_flag AS icp_meets_success_criteria_flag__c,
  icp_score_date AS icp_last_updated__c,
  SYSDATE() as updated_at
FROM all_accounts
LEFT JOIN pte_scores
          ON all_accounts.crm_account_id = pte_scores.crm_account_id
LEFT JOIN ptc_scores
          ON all_accounts.crm_account_id = ptc_scores.crm_account_id
LEFT JOIN icp_scores
          ON all_accounts.crm_account_id = icp_scores.crm_account_id
