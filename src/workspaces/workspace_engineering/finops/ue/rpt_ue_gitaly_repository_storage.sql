
{{ config(
    materialized='table',
    )
}}

with cloud_data as (

    SELECT date_day,
    pricing_unit,
    sum(usage_amount_in_pricing_units) as usage_amount_in_pricing_units,
    sum(net_cost) as net_cost
    FROM {{ ref ('rpt_gcp_billing_pl_day_ext')}}
    where level_2 = 'Repository Storage'
    and gcp_sku_description IN ('SSD backed PD Capacity', 'Storage PD Snapshot in US', 'Balanced PD Capacity', 'Storage PD Capacity')
    and date_day >= '2023-02-01'
    group by 1,2
    order by 1 desc

),

repo_storage_ratio_daily AS (

  SELECT
    prep_gitlab_dotcom_project_statistics_daily_snapshot.snapshot_day                               AS date_day,
    COALESCE(prep_gitlab_dotcom_project_statistics_daily_snapshot.finance_pl_category, 'internal')  AS finance_pl_category,
    SUM(prep_gitlab_dotcom_project_statistics_daily_snapshot.repo_size_gb)                          AS repo_size_gb,
    RATIO_TO_REPORT(SUM(prep_gitlab_dotcom_project_statistics_daily_snapshot.repo_size_gb)) 
      OVER (PARTITION BY prep_gitlab_dotcom_project_statistics_daily_snapshot.snapshot_day)         AS percent_repo_size_gb
  FROM {{ ref('prep_gitlab_dotcom_project_statistics_daily_snapshot') }}
  GROUP BY
    1, 2

),

gitlab_data as (

    SELECT date_day as date_day,
    sum(repo_size_gb) as gitlab_repo_size_gb
    FROM repo_storage_ratio_daily
    WHERE date_day >= '2023-02-01'
    group by 1

),

joined as (

    SELECT c.date_day,
    c.pricing_unit,
    c.usage_amount_in_pricing_units,
    c.net_cost,
    g.gitlab_repo_size_gb,
    c.usage_amount_in_pricing_units * 30.41 - g.gitlab_repo_size_gb as overhead_gb,
    c.usage_amount_in_pricing_units * 30.41/g.gitlab_repo_size_gb as overhead_percent,
    c.net_cost/(g.gitlab_repo_size_gb/1024) as monthly_unit_price_per_repo_tb
    FROM cloud_data c
    LEFT JOIN gitlab_data g
    ON c.date_day = g.date_day

)

SELECT * FROM joined
order by 1 desc