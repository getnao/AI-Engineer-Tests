{{ simple_cte([
    ('pl_combined', 'mart_gcp_billing_line_item'),
    ('lookback_pl_mappings', 'rpt_gcp_billing_lookback'),
    ('gcp_billing_hierarchy', 'gcp_billing_hierarchy')
]) }}


, split_by_pl_pct AS (

  SELECT
    pl_combined.date_day,
    pl_combined.gcp_project_id,
    pl_combined.gcp_service_description,
    pl_combined.gcp_sku_description,
    pl_combined.infra_label,
    pl_combined.env_label,
    pl_combined.runner_label,
    pl_combined.full_path,
    pl_combined.plan_name,
    COALESCE(lookback_pl_mappings.pl_category, pl_combined.pl_category)                      AS pl_category,
    pl_combined.usage_unit,
    pl_combined.pricing_unit,
    pl_combined.usage_amount * COALESCE(lookback_pl_mappings.pl_percent, 1)                  AS usage_amount,
    pl_combined.usage_amount_in_pricing_units * COALESCE(lookback_pl_mappings.pl_percent, 1) AS usage_amount_in_pricing_units,
    pl_combined.cost_before_credits * COALESCE(lookback_pl_mappings.pl_percent, 1)           AS cost_before_credits,
    pl_combined.net_cost * COALESCE(lookback_pl_mappings.pl_percent, 1)                      AS net_cost,
    COALESCE(lookback_pl_mappings.from_mapping, pl_combined.from_mapping)                    AS from_mapping,
    gcp_billing_pl_allocation_pk,
    DENSE_RANK() OVER (
      PARTITION BY
        pl_combined.date_day,
        pl_combined.gcp_project_id,
        pl_combined.gcp_service_description,
        pl_combined.gcp_sku_description,
        pl_combined.infra_label,
        pl_combined.env_label,
        pl_combined.runner_label,
        pl_combined.full_path
      ORDER BY
        (CASE WHEN lookback_pl_mappings.full_path IS NOT NULL 
            THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.gcp_service_description IS NOT NULL 
            THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.gcp_sku_description IS NOT NULL 
            THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.infra_label IS NOT NULL 
            THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.env_label IS NOT NULL 
            THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.runner_label IS NOT NULL 
            THEN 1 ELSE 0 END) DESC,
        (CASE WHEN lookback_pl_mappings.gcp_project_id IS NOT NULL 
            THEN 1 ELSE 0 END) DESC
    )                                                                                        AS priority
  FROM
    pl_combined
  LEFT JOIN lookback_pl_mappings ON pl_combined.date_day = lookback_pl_mappings.date_day
    AND COALESCE(pl_combined.gcp_project_id, 'null') LIKE COALESCE(lookback_pl_mappings.gcp_project_id, COALESCE(pl_combined.gcp_project_id, ''))
    AND COALESCE(lookback_pl_mappings.gcp_service_description, pl_combined.gcp_service_description) = pl_combined.gcp_service_description
    AND COALESCE(lookback_pl_mappings.gcp_sku_description, pl_combined.gcp_sku_description) = pl_combined.gcp_sku_description
    AND COALESCE(lookback_pl_mappings.infra_label, COALESCE(pl_combined.infra_label, '')) = COALESCE(pl_combined.infra_label, '')
    AND COALESCE(lookback_pl_mappings.env_label, COALESCE(pl_combined.env_label, '')) = COALESCE(pl_combined.env_label, '')
    AND COALESCE(lookback_pl_mappings.runner_label, COALESCE(pl_combined.runner_label, '')) = COALESCE(pl_combined.runner_label, '')
    AND COALESCE(lookback_pl_mappings.full_path, COALESCE(pl_combined.full_path, '')) = COALESCE(pl_combined.full_path, '')

),

grouping AS (

  SELECT
    gcp_billing_pl_allocation_pk,
    date_day,
    gcp_project_id,
    gcp_service_description,
    gcp_sku_description,
    infra_label,
    env_label,
    runner_label,
    full_path,
    plan_name,
    pl_category,
    usage_unit,
    pricing_unit,
    from_mapping,
    SUM(usage_amount)                  AS usage_amount,
    SUM(usage_amount_in_pricing_units) AS usage_amount_in_pricing_units,
    SUM(cost_before_credits)           AS cost_before_credits,
    SUM(net_cost)                      AS net_cost
  FROM split_by_pl_pct
  WHERE priority = 1
  {{ dbt_utils.group_by(n=14) }}

),

join_product_component AS (

  SELECT
    *,
    CASE
      WHEN from_mapping LIKE '%ci_runner_pl_daily%'
        OR from_mapping LIKE 'build_artifacts%'
        OR infra_label IN ('continuous_integration', 'runner_saas', 'build_artifacts') 
          THEN 'Production: Continuous Integration'
      WHEN from_mapping LIKE '%haproxy-%' OR infra_label = 'pages' 
        THEN 'Production: HAProxy'
      WHEN from_mapping LIKE 'repo_storage%' OR infra_label IN ('git_lfs', 'gitaly') 
        THEN 'Production: Gitaly'
      WHEN from_mapping LIKE 'container_registry%' OR infra_label IN ('container_registry', 'registry') 
        THEN 'Production: Container Registry'
      WHEN
        from_mapping IN ('folder_pl', 'projects_pl')
        OR infra_label = 'security'
        OR gcp_project_id LIKE ANY ('gitlab-ci-private-%', 'gitlab-r-saas-l-m-amd64-org-%', 'gitlab-r-saas-l-p-amd64-%')
        OR infra_label = 'infrastructure' 
          THEN 'R&D - Staging, Ops, Quality, Security, Demos, Sandboxes'
      WHEN gcp_project_id = 'unreview-poc-390200e5' 
        THEN 'AI'
      WHEN infra_label = 'dependency_proxy' 
        THEN 'Production: Dependency Proxy'
      WHEN infra_label = 'shared' AND from_mapping = 'infralabel_pl' 
        THEN 'WIP: Unallocated Production costs'
      WHEN infra_label IS NULL AND from_mapping IS NULL
        
          THEN
          CASE WHEN gcp_project_id != 'gitlab-production' 
            THEN 'WIP: Unallocated costs'
            WHEN gcp_project_id = 'gitlab-production' 
              THEN 'WIP: Unallocated Production costs'
          END
      ELSE CONCAT(infra_label, '-', from_mapping)
    END AS product_component
  FROM grouping

),

join_finance_component AS (

  SELECT
    *,
    CASE
      -- STORAGE
      WHEN LOWER(gcp_service_description) = 'compute engine' AND LOWER(gcp_sku_description) LIKE '%pd capacity%' 
        THEN 'Storage'
      WHEN LOWER(gcp_service_description) = 'compute engine' AND LOWER(gcp_sku_description) LIKE '%pd snapshot%' 
        THEN 'Storage'
      WHEN (LOWER(gcp_service_description) = 'bigquery' AND LOWER(gcp_sku_description) LIKE '%storage%') 
        THEN 'Storage'
      WHEN (LOWER(gcp_service_description) = 'cloud sql' AND LOWER(gcp_sku_description) LIKE '%storage%') 
        THEN 'Storage'

      -- COMPUTE
      WHEN LOWER(gcp_sku_description) LIKE '%commitment%' 
        THEN 'Committed Usage'--keep
      WHEN LOWER(gcp_service_description) = 'vertex ai' 
        THEN 'AI/ML'--new category
      WHEN LOWER(gcp_service_description) = 'compute engine' AND (LOWER(gcp_sku_description) LIKE '%gpu%' OR LOWER(gcp_sku_description) LIKE '%prediction%') 
        THEN 'AI/ML'--new category
      WHEN LOWER(gcp_service_description) = 'kubernetes engine' 
        THEN 'Compute'
      WHEN LOWER(gcp_service_description) = 'bigquery' AND LOWER(gcp_sku_description) NOT LIKE 'storage' 
        THEN 'Compute'
      WHEN LOWER(gcp_service_description) = 'cloud sql' AND (LOWER(gcp_sku_description) LIKE '%cpu%' OR LOWER(gcp_sku_description) LIKE '%ram%') 
        THEN 'Compute'
      WHEN LOWER(gcp_service_description) LIKE '%memorystore%' AND LOWER(gcp_sku_description) LIKE '%capacity%' 
        THEN 'Compute'

      -- NETWORKING
      WHEN LOWER(gcp_service_description) = 'cloud sql' AND LOWER(gcp_sku_description) LIKE '%networking%' 
        THEN 'Networking'
      WHEN LOWER(gcp_service_description) = 'cloud pub/sub' 
        THEN 'Networking'--keep
      WHEN LOWER(gcp_service_description) LIKE '%memorystore%' AND LOWER(gcp_sku_description) LIKE '%networking%' 
        THEN 'Networking'
      WHEN LOWER(gcp_sku_description) LIKE '%load balanc%' 
        THEN 'Networking'
      WHEN LOWER(gcp_service_description) = 'compute engine' AND (LOWER(gcp_sku_description) LIKE '%ip%' OR LOWER(gcp_sku_description) LIKE '%network%' OR LOWER(gcp_sku_description) LIKE '%upload%' OR LOWER(gcp_sku_description) LIKE '%download%') 
        THEN 'Networking'--keep
      WHEN LOWER(gcp_service_description) = 'cloud storage' AND (LOWER(gcp_sku_description) LIKE '%network%' OR LOWER(gcp_sku_description) LIKE '%download%' OR LOWER(gcp_sku_description) LIKE '%cdn%') 
        THEN 'Networking'

      -- SUPPORT
      WHEN LOWER(gcp_sku_description) LIKE '%support%' 
        THEN 'Support'
      WHEN LOWER(gcp_sku_description) LIKE '%security command center%' 
        THEN 'Support'
      WHEN LOWER(gcp_sku_description) LIKE '%marketplace%' 
        THEN 'Support'

      -- OTHERS
      WHEN LOWER(gcp_service_description) = 'cloud storage' 
        THEN 'Storage'
      WHEN LOWER(gcp_service_description) = 'compute engine' 
        THEN 'Compute'
      ELSE gcp_service_description
    END AS finance_sku_type,

    CASE
      -- STORAGE
      WHEN LOWER(gcp_service_description) = 'cloud storage' AND ((LOWER(gcp_sku_description) LIKE '%standard storage%') OR (LOWER(gcp_sku_description) LIKE '%coldline storage%') OR (LOWER(gcp_sku_description) LIKE '%archive storage%') OR (LOWER(gcp_sku_description) LIKE '%nearline storage%')) 
        THEN 'Object (storage)'
      WHEN LOWER(gcp_service_description) = 'cloud storage' AND LOWER(gcp_sku_description) LIKE '%operations%' 
        THEN 'Object (operations)'
      WHEN LOWER(gcp_service_description) = 'compute engine' AND (LOWER(gcp_sku_description) LIKE '%pd capacity%' OR LOWER(gcp_sku_description) LIKE '%pd snapshot%') 
        THEN 'Repository (storage) - to be refined'
      WHEN (LOWER(gcp_service_description) = 'bigquery' AND LOWER(gcp_sku_description) LIKE '%storage%') 
        THEN 'Data Warehouse (storage)'
      WHEN (LOWER(gcp_service_description) = 'cloud sql' AND LOWER(gcp_sku_description) LIKE '%storage%') 
        THEN 'Databases (storage)'

      -- COMPUTE
      WHEN LOWER(gcp_sku_description) LIKE '%commitment%' 
        THEN 'Committed Usage' --keep
      WHEN LOWER(gcp_service_description) = 'kubernetes engine' 
        THEN 'Container orchestration (compute)'--keep
      WHEN LOWER(gcp_service_description) = 'vertex ai' 
        THEN 'AI/ML (compute)'
      WHEN LOWER(gcp_service_description) = 'compute engine' AND LOWER(gcp_sku_description) LIKE '%gpu%' 
        THEN 'AI/ML (compute)'
      WHEN LOWER(gcp_service_description) LIKE 'bigquery' AND LOWER(gcp_sku_description) NOT LIKE 'storage' 
        THEN 'Data Warehouse (compute)'
      WHEN LOWER(gcp_service_description) LIKE 'cloud sql' AND (LOWER(gcp_sku_description) LIKE '%cpu%' OR LOWER(gcp_sku_description) LIKE '%ram%') 
        THEN 'Data Warehouse (compute)'
      WHEN LOWER(gcp_service_description) LIKE '%memorystore%' AND LOWER(gcp_sku_description) LIKE '%capacity%' 
        THEN 'Memorystores (compute)'

      -- NETWORKING
      WHEN LOWER(gcp_service_description) = 'cloud sql' AND LOWER(gcp_sku_description) LIKE '%networking%' 
        THEN 'Databases (networking)'
      WHEN LOWER(gcp_service_description) = 'cloud pub/sub' 
        THEN 'Messaging (networking)'--keep
      WHEN LOWER(gcp_service_description) LIKE '%memorystore%' AND LOWER(gcp_sku_description) LIKE '%networking%' 
        THEN 'Memorystores (networking)'
      WHEN LOWER(gcp_service_description) = 'cloud storage' AND LOWER(gcp_sku_description) LIKE '%egress%' 
        THEN CASE WHEN LOWER(gcp_sku_description) LIKE '%multi-region within%' 
        THEN 'Object (networking)' ELSE 'Object (networking) - to be refined' END --keep
      WHEN LOWER(gcp_service_description) = 'cloud storage' AND LOWER(gcp_sku_description) LIKE '%cdn%' 
        THEN CASE WHEN LOWER(gcp_sku_description) NOT LIKE '%from%' 
        THEN 'Object CDN (networking)' ELSE 'Object CDN (networking) - to be refined' END--keep
      WHEN LOWER(gcp_service_description) = 'cloud storage' AND (LOWER(gcp_sku_description) LIKE '%network%' OR LOWER(gcp_sku_description) LIKE '%download%') 
        THEN 'Networking on Buckets'--rename
      WHEN LOWER(gcp_sku_description) LIKE '%load balanc%' 
        THEN 'Load Balancing (networking)'
      WHEN LOWER(gcp_service_description) = 'networking' 
        THEN 'Networking (mixed) - to be refined'

      -- SUPPORT
      WHEN LOWER(gcp_sku_description) LIKE '%support%' 
        THEN 'Google Support (support)'
      WHEN LOWER(gcp_sku_description) LIKE '%security command center%' 
        THEN 'Security (support)'
      WHEN LOWER(gcp_sku_description) LIKE '%marketplace%' 
        THEN 'Marketplace (support)'

      -- OTHERS
      WHEN LOWER(gcp_service_description) = 'cloud storage' 
        THEN 'Storage'
      WHEN LOWER(gcp_service_description) = 'compute engine' 
        THEN 'Compute'
      ELSE gcp_service_description
    END AS finance_sku_subtype
  FROM join_product_component

),

join_hierarchy_component AS (

  SELECT
    join_finance_component.*,
    level_0,
    level_1,
    level_2,
    level_3,
    level_4
  FROM join_finance_component
  LEFT JOIN gcp_billing_hierarchy
    ON COALESCE(join_finance_component.full_path, '') LIKE COALESCE(gcp_billing_hierarchy.full_path, '')
      AND COALESCE(join_finance_component.from_mapping, '') = COALESCE(gcp_billing_hierarchy.from_mapping, '')
      AND (COALESCE(join_finance_component.infra_label, '') = COALESCE(gcp_billing_hierarchy.infra_label, '') OR gcp_billing_hierarchy.infra_label IS NULL)
      AND (COALESCE(join_finance_component.gcp_project_id, '') = COALESCE(gcp_billing_hierarchy.gcp_project_id, '') OR gcp_billing_hierarchy.gcp_project_id IS NULL)

)

SELECT *
FROM join_hierarchy_component
