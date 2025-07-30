{{ config({
    "tags": ["six_hourly"],
    "materialized": "incremental",
    "unique_key": "dim_crm_opportunity_partner_sk",
    "on_schema_change": "sync_all_columns",
    "post-hook": "{{ missing_member_column(primary_key = 'dim_crm_opportunity_partner_sk', not_null_test_cols = []) }}"
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
        dr_partner_deal_type,
        dr_partner_engagement,         
        dr_status,
        dr_primary_registration,
        dr_deal_id,
        aggregate_partner,
        partner_initiated_opportunity,
        calculated_partner_track,
        partner_account,
        partner_discount,
        partner_discount_calc,
        partner_margin_percentage,
        partner_track,
        platform_partner,
        resale_partner_track,
        resale_partner_name,
        fulfillment_partner,
        fulfillment_partner_account_name, 
        fulfillment_partner_partner_track,
        partner_account_account_name,
        partner_account_partner_track,
        influence_partner,
        comp_channel_neutral,
        distributor
    FROM prep_crm_opportunity

),

final AS (

    SELECT 
        {{ dbt_utils.generate_surrogate_key(get_opportunity_partner_fields()) }} AS dim_crm_opportunity_partner_sk,
        *
    FROM distinct_values
    
)
SELECT *
FROM final