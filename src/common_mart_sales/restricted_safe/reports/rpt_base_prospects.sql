{{ config(
    tags=["six_hourly"]
) }}

{{ simple_cte([
    ('opportunities', 'mart_crm_opportunity'),
    ('dim_crm_account', 'dim_crm_account'),
    ('fy25_base_prospects', 'prep_base_prospect_fy25')
]) }},

combined_base_prospect_segments AS (

  SELECT 
    opportunities.dim_crm_opportunity_id,
    opportunities.close_date,
    opportunities.close_fiscal_year,
    opportunities.close_fiscal_quarter_name,
    opportunities.report_segment,
    opportunities.is_mid_market_plus,
    dim_crm_account.dim_crm_account_id,
    dim_crm_account.is_base_prospect_account                AS is_base_prospect_account_fy26,
    fy25_base_prospects.is_base_prospect_account_fy25,
    CASE
    -- Base prospect accounts may have old closed lost opps, or old churned customers with associated opps. Those are not base.
      WHEN opportunities.close_fiscal_year < 2025
        OR opportunities.close_fiscal_year IS NULL
        THEN opportunities.report_segment

     -- fy25 opportunities
      WHEN opportunities.close_fiscal_year = 2025
        AND opportunities.report_segment != 'SMB' 
        AND opportunities.report_segment IS NOT NULL
        THEN opportunities.report_segment
      -- > 4000 employees but IT spend < $250,000  
      WHEN opportunities.close_fiscal_year = 2025
        AND (opportunities.report_segment = 'SMB' OR opportunities.report_segment IS NULL)
        AND fy25_base_prospects.is_base_prospect_account_fy25 = TRUE
        AND fy25_base_prospects.parent_crm_account_max_family_employee > 4000 
        AND (fy25_base_prospects.it_spend <= 250000 OR fy25_base_prospects.it_spend IS NULL)
        THEN 'BASE - LARGE'
      WHEN opportunities.close_fiscal_year = 2025
        AND (opportunities.report_segment = 'SMB' OR opportunities.report_segment IS NULL)
        AND fy25_base_prospects.is_base_prospect_account_fy25 = TRUE
        AND (fy25_base_prospects.parent_crm_account_max_family_employee <= 4000 
          OR fy25_base_prospects.parent_crm_account_max_family_employee IS NULL)
        THEN 'BASE - MID-MARKET'
         -- if the base prospect box is not true (F/Null), then FY25 opps need to go into SMB - minus base
      WHEN opportunities.close_fiscal_year = 2025
        AND opportunities.report_segment = 'SMB' 
        AND (fy25_base_prospects.is_base_prospect_account_fy25 = FALSE
          OR fy25_base_prospects.is_base_prospect_account_fy25 IS NULL)
        THEN 'SMB - MINUS BASE' 

    -- fy26 opportunities use the segmentation in mart_crm_opportunity
      WHEN opportunities.close_fiscal_year >=  2026
        THEN opportunities.base_prospect_report_segment END AS base_prospect_report_segment_combined
  FROM opportunities 
  LEFT JOIN dim_crm_account
      ON opportunities.dim_crm_account_id = dim_crm_account.dim_crm_account_id
  LEFT JOIN fy25_base_prospects
      ON opportunities.dim_crm_account_id = fy25_base_prospects.dim_crm_account_id

),

final AS (

  SELECT 
    dim_crm_opportunity_id,
    dim_crm_account_id,
    report_segment,
    close_date,
    close_fiscal_quarter_name,
    close_fiscal_year,
    is_base_prospect_account_fy26,
    is_base_prospect_account_fy25,
    base_prospect_report_segment_combined,
    is_mid_market_plus
  FROM combined_base_prospect_segments 

)
SELECT *
FROM final

