{{ config(
    tags=["mnpi", "six_hourly"]
) }}

WITH source AS (

  SELECT *
  FROM {{ ref('sfdc_opportunity_snapshots_source') }}

),

-- source is filtered for only relevant opportunities and aggregated to reduce rows to the minimum required.
aggregated_source_decommissioned_opps AS (

  SELECT
    opportunity_id,
    opportunity_id_to_decommission,
    MIN(dbt_valid_from)                                                          AS dbt_valid_from,
    MAX({{ coalesce_to_infinity('dbt_valid_to') }})                              AS dbt_valid_to
  FROM source
  WHERE opportunity_id_to_decommission IS NOT NULL
  GROUP BY 1, 2

),

-- order_type of decommissioned opportunity_id is retrieved by joining the source table with the aggregated cte above.
order_type_of_decommissioned_opportunity_cte_prep AS (

  SELECT
    filtered.opportunity_id,
    order_type_of_decommissioned_opps.dbt_valid_from,
    order_type_of_decommissioned_opps.dbt_valid_to,
    order_type_of_decommissioned_opps.order_type_stamped                         AS order_type_of_decommissioned_opportunity

  FROM aggregated_source_decommissioned_opps AS filtered
  LEFT JOIN source AS order_type_of_decommissioned_opps
    ON filtered.opportunity_id_to_decommission = order_type_of_decommissioned_opps.opportunity_id
    AND (
      -- Case 1: The filtered record starts during the decommissioned opp's validity period
      (filtered.dbt_valid_from >= order_type_of_decommissioned_opps.dbt_valid_from AND filtered.dbt_valid_from < {{ coalesce_to_infinity('order_type_of_decommissioned_opps.dbt_valid_to') }})
      OR
      -- Case 2: The filtered record ends during the decommissioned opp's validity period
      (filtered.dbt_valid_to > order_type_of_decommissioned_opps.dbt_valid_from AND filtered.dbt_valid_to <= {{ coalesce_to_infinity('order_type_of_decommissioned_opps.dbt_valid_to') }})
      OR
      -- Case 3: The filtered record's validity period fully encompasses the decommissioned opp's period
      (filtered.dbt_valid_from <= order_type_of_decommissioned_opps.dbt_valid_from AND filtered.dbt_valid_to >= {{ coalesce_to_infinity('order_type_of_decommissioned_opps.dbt_valid_to') }})
    )

),

-- order_type_of_decommissioned_opportunity can be changed back throughout the period (A to B and back to A). This would result issues when simply grouping by opportunity_id and order_type_of_decommissioned_opportunity
-- To resolve this issue, another grouping field (group_id) is created, using LAG window function.
order_type_decom_opp_groups AS (

  SELECT
    opportunity_id,
    dbt_valid_from,
    dbt_valid_to,
    order_type_of_decommissioned_opportunity,
    IFF(
      order_type_of_decommissioned_opportunity 
        = LAG(order_type_of_decommissioned_opportunity) OVER (PARTITION BY opportunity_id ORDER BY dbt_valid_from),
      0, 1)                                                                       AS is_new_group
  FROM order_type_of_decommissioned_opportunity_cte_prep

),

numbered_groups AS (

  SELECT
    *,
    SUM(is_new_group) OVER (PARTITION BY opportunity_id ORDER BY dbt_valid_from)  AS group_id 
  FROM order_type_decom_opp_groups

),

aggregated_final AS (

  SELECT
    opportunity_id,
    order_type_of_decommissioned_opportunity,
    MIN(dbt_valid_from)                                                           AS dbt_valid_from,
    MAX({{ coalesce_to_infinity('dbt_valid_to') }})                               AS dbt_valid_to
  FROM numbered_groups
  GROUP BY
    opportunity_id,
    group_id,
    order_type_of_decommissioned_opportunity

),

-- LAST_VALUE function and SELECT DISTINCT operation sare performed because one decommissioned opportunity_id may be associated with multiple order_type during the same time period. 
joined_and_deduplicated  AS (

  SELECT DISTINCT
    source.opportunity_id,
    source.dbt_valid_from,
    source.dbt_valid_to,
    LAST_VALUE(order_type_of_decommissioned_opportunity) OVER (
      PARTITION BY source.opportunity_id
      ORDER BY source.dbt_valid_from ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                                                            AS order_type_of_decommissioned_opportunity
  FROM source
  LEFT JOIN aggregated_final AS decommissioned_opps
    ON source.opportunity_id = decommissioned_opps.opportunity_id
    AND (
      -- Case 1: The source record starts during the decommissioned opp's validity period
      (source.dbt_valid_from >= decommissioned_opps.dbt_valid_from AND source.dbt_valid_from < {{ coalesce_to_infinity('decommissioned_opps.dbt_valid_to') }})
      OR
      -- Case 2: The source record ends during the decommissioned opp's validity period
      (source.dbt_valid_to > decommissioned_opps.dbt_valid_from AND source.dbt_valid_to <= {{ coalesce_to_infinity('decommissioned_opps.dbt_valid_to') }})
      OR
      -- Case 3: The source record's validity period fully encompasses the decommissioned opp's period
      (source.dbt_valid_from <= decommissioned_opps.dbt_valid_from AND source.dbt_valid_to >= {{ coalesce_to_infinity('decommissioned_opps.dbt_valid_to') }})
    )

)

SELECT *
FROM joined_and_deduplicated