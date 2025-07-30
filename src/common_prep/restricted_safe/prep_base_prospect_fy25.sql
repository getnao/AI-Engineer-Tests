{{
  config(
    materialized = 'incremental',
    unique_key = 'dim_crm_account_id',
    on_schema_change = 'sync_all_columns'
  )
}}

{% if is_incremental() %}

  -- This section runs on subsequent runs, but returns no rows
  -- effectively making this a static table after the first run
  -- evenutually we can consider converting this into a seed once 
  -- stakeholders are satisfied with the way base accounts are classified
  SELECT *
  FROM {{this}}  
  WHERE 1 = 0  -- This ensures no rows are returned

{% else %}

WITH prep_crm_account_daily_snapshot AS (

  SELECT *
  FROM {{ ref('prep_crm_account_daily_snapshot') }}
  WHERE snapshot_date = '2024-02-01' -- this is the date used to evaluate whether an account was a base account in that year

),

segmenting_accounts AS (

  -- This section runs only on the first run
  SELECT 
    dim_crm_account_id,
    parent_crm_account_max_family_employee,
    it_spend,
    CASE
      -- Don't consider accounts created in FY25
      WHEN crm_account_created_date = '2024-02-01' 
        THEN 'Not Base Prospect - account new in FY25'

      -- First check: Less than 250 employees
      WHEN parent_crm_account_max_family_employee < 250 
          THEN 'Not Base Prospect - <250 Employees'
      
      -- Check for exclusions - customer status
      WHEN crm_account_type = 'Customer' 
          THEN 'Not Base Prospect - Existing Customer'
      
      -- Check for exclusions - public sector
      WHEN pubsec_type IS NOT NULL 
          THEN 'Not Base Prospect - PubSec'
      
      -- Check for regional exclusions
      WHEN parent_crm_account_geo = 'APJ' 
          THEN 'Not Base Prospect - APJ'
      WHEN parent_crm_account_upa_country_name IN ('RUS', 'EGC', 'META', 'LATAM') 
          THEN 'Not Base Prospect - Excluded Region'
      WHEN parent_crm_account_upa_country_name IN ('Israel', 'Italy', 'Spain', 'Portugal') 
          THEN 'Not Base Prospect - ITIB/IL'
      WHEN parent_crm_account_geo = 'AMER' 
        AND parent_crm_account_industry = 'Finance' 
          THEN 'Not Base Prospect - AMER FinServ'
      WHEN parent_crm_account_region = 'TELCO' 
          THEN 'Not Base Prospect - TELCO'
      
      -- Check for target industry 
      WHEN parent_crm_account_industry NOT IN (
        'Aerospace & Defense', 'Automotive', 'Banking', 'Business Services', 'Education',
        'Finance', 'Government', 'Hospitality', 'Insurance', 'Internet Software & Services',
        'Machinery', 'Manufacturing', 'Media', 'Other', 'Retail',
        'Technology', 'Telecommunications', 'Transportation') 
          THEN 'Not Base Prospect - Non-Target Industry'
      
      -- For larger companies (4000+ employees) with IT spend < $250,000
      WHEN parent_crm_account_max_family_employee > 4000 
        AND (it_spend <= 250000 OR it_spend IS NULL)
        THEN 'BASE - LARGE'
      
      -- For mid-sized companies (250-3999 employees)
      WHEN parent_crm_account_max_family_employee BETWEEN 250 AND 4000 
        AND (parent_crm_account_lam_dev_count IS NULL OR parent_crm_account_lam_dev_count < 80)
          THEN 'BASE - MID-MARKET'
      WHEN parent_crm_account_max_family_employee BETWEEN 250 AND 4000 
        AND parent_crm_account_lam_dev_count >= 80
          THEN 'Not Base Prospect - High LAM Dev Count'
      
      -- Catch all, to be determined what we call this
      ELSE 'Excluded' 
      END AS base_prospect_type_fy25
  FROM prep_crm_account_daily_snapshot

),

final AS (

  SELECT
    dim_crm_account_id,
    parent_crm_account_max_family_employee,
    it_spend,
    base_prospect_type_fy25,
    IFF(base_prospect_type_fy25 IN ('BASE - MID-MARKET', 'BASE - LARGE'), TRUE, FALSE) AS is_base_prospect_account_fy25 -- Boolean flag used downstream
  FROM segmenting_accounts

)
SELECT *
FROM final

{% endif %}
