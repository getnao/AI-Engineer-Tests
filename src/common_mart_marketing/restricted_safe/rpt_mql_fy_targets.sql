{{ config(
    materialized="table"
) }}

{{ simple_cte([
    ('sheetload_fy_mql_targets_source','sheetload_fy_mql_targets_source'),
    ('dim_date','dim_date')
]) }}

, final AS (

  SELECT DISTINCT
    sheetload_fy_mql_targets_source.fiscal_quarter,
    sheetload_fy_mql_targets_source.report_region,
    sheetload_fy_mql_targets_source.report_geo,
    CASE 
      WHEN fiscal_quarter_name_fy < current_fiscal_quarter_name_fy 
        THEN 1
      WHEN fiscal_quarter_name_fy = current_fiscal_quarter_name_fy 
        THEN (dim_date.current_date_actual - dim_date.first_day_of_fiscal_quarter) / 
          (dim_date.last_day_of_fiscal_quarter - dim_date.first_day_of_fiscal_quarter)
      ELSE 0 
    END AS pct_qtr_done,
    sheetload_fy_mql_targets_source.target AS mql_target
  FROM sheetload_fy_mql_targets_source
  INNER JOIN dim_date
    ON sheetload_fy_mql_targets_source.fiscal_quarter = dim_date.fiscal_quarter_name_fy
      AND day_of_month = 1

)

SELECT *
FROM final