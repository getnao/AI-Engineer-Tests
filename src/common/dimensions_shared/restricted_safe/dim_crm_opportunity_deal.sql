{{ config({
    "tags": ["six_hourly"],
    "materialized": "incremental",
    "unique_key": "dim_crm_opportunity_deal_sk",
    "on_schema_change": "sync_all_columns",
    "post-hook": "{{ missing_member_column(primary_key = 'dim_crm_opportunity_deal_sk', not_null_test_cols = []) }}"
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

      deal_path,
      opportunity_deal_size,
      deal_category,
      deal_group,
      deal_size,
      calculated_deal_size,
      deal_size_bucket,
      deal_path_engagement

    FROM prep_crm_opportunity

),

final AS (

    SELECT 
        {{ dbt_utils.generate_surrogate_key(get_opportunity_deal_fields()) }} AS dim_crm_opportunity_deal_sk,
        *
    FROM distinct_values
    
)

SELECT *
FROM final