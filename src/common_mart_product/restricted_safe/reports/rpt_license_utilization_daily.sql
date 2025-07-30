{{
  config(
    materialized='table'
  )
}}

WITH pivoted AS (

  SELECT 
    report_date             AS reporting_date,
    dim_installation_id,
    dim_namespace_id,
    namespace_is_internal,
    is_duo_pro_trial,
    is_duo_enterprise_trial,
    {{ dbt_utils.pivot(
          "UPPER(REPLACE(assignable_feature_set,' ', '_'))", 
          dbt_utils.get_column_values(ref('mart_license_utilization_daily'),column="UPPER(REPLACE(assignable_feature_set,' ', '_'))", where="assignable_feature_set IS NOT NULL"),
          agg='sum',
          then_value='license_users',
          else_value="NULL",
          suffix='_LICENSE_USERS'
      ) }},
    {{ dbt_utils.pivot(
          "UPPER(REPLACE(assignable_feature_set,' ', '_'))", 
          dbt_utils.get_column_values(ref('mart_license_utilization_daily'),column="UPPER(REPLACE(assignable_feature_set,' ', '_'))", where="assignable_feature_set IS NOT NULL"),
          agg='sum',
          then_value='billable_users',
          else_value="NULL",
          suffix='_BILLABLE_USERS'
      ) }}
  FROM {{ ref('mart_license_utilization_daily') }}
  GROUP BY ALL

)
SELECT *
FROM pivoted
WHERE duo_pro_license_users IS NOT NULL
  OR duo_enterprise_license_users IS NOT NULL
  OR duo_with_amazon_q_license_users IS NOT NULL
  OR enterprise_agile_planning_license_users IS NOT NULL