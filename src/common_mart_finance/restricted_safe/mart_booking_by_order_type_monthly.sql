{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('dim_crm_opportunity_flags', 'dim_crm_opportunity_flags'),
    ('dim_crm_opportunity_source_and_path', 'dim_crm_opportunity_source_and_path'),
    ('dim_date', 'dim_date')
]) }},

fct_crm_opportunity AS (

  SELECT *
  FROM {{ ref('fct_crm_opportunity', v=2) }}

),
  
dim_crm_opportunity AS (

  SELECT *
  FROM {{ ref('dim_crm_opportunity', v=2) }}

),

basis AS (

/* Booking amount and count by subscription type */

  SELECT
    DATE(DATE_TRUNC('month', fct_crm_opportunity.close_date)) AS period,
    dim_crm_opportunity_source_and_path.subscription_type AS subscription_type,
    SUM(fct_crm_opportunity.amount)                 AS opportunity_amount,
    COUNT(fct_crm_opportunity.amount)               AS opportunity_count
  FROM fct_crm_opportunity
  LEFT JOIN dim_crm_opportunity 
    ON fct_crm_opportunity.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN dim_crm_opportunity_flags
    ON fct_crm_opportunity.dim_crm_opportunity_flags_sk = dim_crm_opportunity_flags.dim_crm_opportunity_flags_sk
  LEFT JOIN dim_crm_opportunity_source_and_path
    ON fct_crm_opportunity.dim_crm_opportunity_source_and_path_sk = dim_crm_opportunity_source_and_path.dim_crm_opportunity_source_and_path_sk
  WHERE dim_crm_opportunity_flags.is_won = TRUE
  {{ dbt_utils.group_by(n=2)}}
  ORDER BY period, subscription_type

),

final AS (

/* Adding fiscal year and quarter */

  SELECT
    --Primary key
    basis.period,

    --Dates
    dim_date.fiscal_year            AS fiscal_year,
    dim_date.fiscal_quarter_name_fy AS fiscal_quarter,

    --Additive fields
    basis.subscription_type         AS subscription_type,

    --Amounts
    basis.opportunity_amount        AS opportunity_amount,
    basis.opportunity_count         AS opportunity_count

  FROM basis
  LEFT JOIN dim_date ON basis.period = dim_date.date_actual

)
SELECT *
FROM final