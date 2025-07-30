{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('sheetload_lead_to_country_code_mapping','sheetload_lead_to_country_code_mapping_source'),
    ('sheetload_lead_country_to_territory_mapping','sheetload_lead_country_to_territory_mapping_source'),
    ('dim_crm_account','dim_crm_account'),
    ('fct_crm_account','fct_crm_account'),
    ('dim_crm_person','dim_crm_person'),
    ('dim_location_country','dim_location_country')
]) }}

, prep AS (

  SELECT
    dim_crm_person.dim_crm_person_id,
    COALESCE(
      fct_crm_account.number_of_employees, 
      dim_crm_person.number_of_employees, 
      1
    )                                                                                                  AS number_of_employees_final,
    IFNULL(
      dim_crm_person.company_address_country, 
      dim_crm_person.country
    )                                                                                                  AS person_country_prep,
    IFNULL(
      dim_crm_person.company_address_state, 
      dim_crm_person.state_code
    )                                                                                                  AS person_state_code,
    IFNULL(
      dim_location_country.country_name, 
      person_country_prep
    )                                                                                                  AS report_country_prep,
    sheetload_lead_to_country_code_mapping.standard_country_name                                       AS report_country,
    sheetload_lead_to_country_code_mapping.country_iso_code                                            AS report_country_iso_2_country_code,
    CASE 
    -- EAST Region States
      WHEN report_country_iso_2_country_code = 'US' AND number_of_employees_final >= 4000 AND UPPER(person_state_code) IN (
        -- Two-letter codes
        'CT', 'MA', 'ME', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT',
        'AL', 'AR', 'AS', 'DC', 'DE', 'FL', 'GA', 'GU', 'KS', 
        'KY', 'LA', 'MD', 'MH', 'MO', 'MS', 'NC', 'NE', 'OH', 
        'OK', 'PR', 'SC', 'TN', 'UM', 'VA', 'WV', 'VI') 
      THEN 'EAST'
    -- Full state names and variations mapping to EAST
      WHEN report_country_iso_2_country_code = 'US' AND number_of_employees_final >= 4000 AND UPPER(person_state_code) IN (
        'CONNECTICUT', 'MASSACHUSETTS', 'MAINE', 'NEW HAMPSHIRE', 
        'NEW JERSEY', 'NEW YORK', 'PENNSYLVANIA', 'RHODE ISLAND', 'VERMONT',
        'ALABAMA', 'ARKANSAS', 'AMERICAN SAMOA', 'WASHINGTON DC', 
        'DELAWARE', 'FLORIDA', 'GEORGIA', 'GUAM', 'KANSAS', 'KENTUCKY', 
        'LOUISIANA', 'MARYLAND', 'MARSHALL ISLANDS', 'MISSOURI', 
        'MISSISSIPPI', 'NORTH CAROLINA', 'NEBRASKA', 'OHIO', 'OHIA',
        'OKLAHOMA', 'PUERTO RICO', 'SOUTH CAROLINA', 'TENNESSEE', 
        'VIRGINIA', 'VIRGINA', 'WEST VIRGINIA') 
      THEN 'EAST'
    
      -- WEST Region States
      WHEN report_country_iso_2_country_code = 'US' AND number_of_employees_final >= 4000 AND UPPER(person_state_code) IN (
        -- Two-letter codes
        'AK', 'HI', 'IA', 'ID', 'IL', 'IN', 'MI', 'MN', 'MT', 
        'ND', 'SD', 'WA', 'WI', 'WY', 'CA', 'AZ', 'CO', 'NM', 
        'NV', 'TX', 'UT', 'OR') 
      THEN 'WEST'
    
    -- Full state names and variations mapping to WEST
      WHEN report_country_iso_2_country_code = 'US' AND number_of_employees_final >= 4000 AND UPPER(person_state_code) IN (
        'ALASKA', 'HAWAII', 'IOWA', 'IDAHO', 'ILLINOIS', 'INDIANA', 
        'MICHIGAN', 'MINNESOTA', 'MONTANA', 'NORTH DAKOTA', 
        'SOUTH DAKOTA', 'WASHINGTON', 'WISCONSIN', 'WYOMING',
        'CALIFORNIA', 'CALIFORINA', 'ARIZONA', 'COLORADO', 
        'NEW MEXICO', 'NEVADA', 'NAVADA', 'TEXAS', 'UTAH', 
        'OREGON', 'OREGAN')
      THEN 'WEST'
    
    -- All other values (non-US locations, null, invalid) return NULL
    ELSE NULL
    END AS ent_region_us,
    CASE 
      WHEN UPPER(dim_crm_account.parent_crm_account_territory) LIKE '%BASE%' 
      THEN TRUE 
      ELSE FALSE 
    END AS is_base_territory, 
    COALESCE(
      dim_crm_account.parent_crm_account_geo, 
      sheetload_lead_country_to_territory_mapping.geo,
      'UNKNOWN'
    )                                                                                                  AS report_geo,
    COALESCE(
      IFF(is_base_territory, 'BASE', dim_crm_account.parent_crm_account_region), 
      sheetload_lead_country_to_territory_mapping.region,
      ent_region_us,
      'UNKNOWN'
    )                                                                                                  AS report_region,
    COALESCE(
      IFF(is_base_territory, dim_crm_account.parent_crm_account_region, dim_crm_account.parent_crm_account_area), 
      sheetload_lead_country_to_territory_mapping.area,
      'UNKNOWN'
    )                                                                                                  AS report_area,
    CASE 
      WHEN dim_crm_account.parent_crm_account_sales_segment IS NOT NULL
        THEN UPPER(dim_crm_account.parent_crm_account_sales_segment) 
      WHEN report_geo IN ('AMER','EMEA', 'APJ') AND report_region = 'COMM' 
        THEN 'MID-MARKET'
      WHEN report_area = 'LATAM' AND number_of_employees_final <= 3999     
        THEN 'MID-MARKET'
      WHEN report_area = 'JPCOMM' 
        THEN 'MID-MARKET'
      WHEN report_geo = 'SMB' AND report_region = 'BASE' 
        THEN 'MID-MARKET'
      WHEN report_geo = 'SMB' 
        THEN 'SMB'
      WHEN report_geo IN ('AMER', 'EMEA', 'APJ') 
        THEN 'LARGE'
      WHEN report_geo = 'JIHU' 
        THEN 'JIHU'
      ELSE 'SMB' 
    END                                                                                                AS report_sales_segment
  FROM dim_crm_person
  LEFT JOIN fct_crm_account
    ON dim_crm_person.dim_crm_account_id = fct_crm_account.dim_crm_account_id
  LEFT JOIN dim_crm_account
    ON dim_crm_person.dim_crm_account_id = dim_crm_account.dim_crm_account_id
  LEFT JOIN dim_location_country
    ON UPPER(person_country_prep) = dim_location_country.iso_2_country_code
  LEFT JOIN sheetload_lead_to_country_code_mapping
    ON UPPER(report_country_prep) = sheetload_lead_to_country_code_mapping.country_name_variant
  LEFT JOIN sheetload_lead_country_to_territory_mapping
    ON report_country_iso_2_country_code = sheetload_lead_country_to_territory_mapping.country_iso_code
      AND number_of_employees_final BETWEEN IFNULL(sheetload_lead_country_to_territory_mapping.min_employees, 0) 
                                        AND sheetload_lead_country_to_territory_mapping.max_employees

), final AS (
  SELECT 
    dim_crm_person_id,
    report_country,
    report_country_iso_2_country_code,
    report_geo,
    report_region,
    report_area,
    report_sales_segment
  FROM prep
)

SELECT *
FROM final