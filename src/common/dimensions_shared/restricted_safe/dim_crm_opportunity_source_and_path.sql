{{ config({
    "tags": ["six_hourly"],
    "materialized": "incremental",
    "unique_key": "dim_crm_opportunity_source_and_path_sk",
    "on_schema_change": "sync_all_columns",
    "post-hook": "{{ missing_member_column(primary_key = 'dim_crm_opportunity_source_and_path_sk', not_null_test_cols = []) }}"
    })
}}

WITH prep_crm_opportunity AS (

    SELECT *
    FROM {{ref('prep_crm_opportunity')}}
    {% if is_incremental() %}

    WHERE snapshot_date >= (CURRENT_DATE - 1)

    {% endif %}

),

distinct_values AS (

    SELECT DISTINCT
      -- Source & Path information
      primary_campaign_source_id,
      generated_source,
      lead_source,
      net_new_source_categories,
      sales_qualified_source,
      sales_qualified_source_grouped,
      sales_path,
      subscription_type,
      source_buckets,
      opportunity_development_representative,
      sdr_or_bdr,
      iqm_submitted_by_role,
      sdr_pipeline_contribution
    FROM prep_crm_opportunity

),

final AS (

    SELECT 
        {{ dbt_utils.generate_surrogate_key(get_opportunity_source_and_path_fields()) }} AS dim_crm_opportunity_source_and_path_sk,
        *
    FROM distinct_values
    
)
SELECT *
FROM final