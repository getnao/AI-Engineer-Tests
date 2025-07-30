{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('crm_accounts', 'dim_crm_account'),
    ('gainsight_instance_info', 'gainsight_instance_info_source'),
    ('prep_host', 'prep_host')
]) }}

, final AS (

    SELECT DISTINCT
      IFF(gainsight_instance_info.instance_uuid IS NOT NULL, {{ dbt_utils.generate_surrogate_key(['prep_host.dim_host_id', 'gainsight_instance_info.instance_uuid'])}}, NULL)  AS dim_installation_id,
      gainsight_instance_info.instance_uuid                                                                                                                                    AS instance_uuid,
      gainsight_instance_info.instance_hostname                                                                                                                                AS instance_hostname,
      gainsight_instance_info.namespace_id                                                                                                                                     AS namespace_id,
      gainsight_instance_info.instance_type                                                                                                                                    AS instance_type,
      gainsight_instance_info.included_in_health_measures_str                                                                                                                  AS included_in_health_measures_str,
      {{ get_keyed_nulls('crm_accounts.dim_crm_account_id')  }}                                                                                                                AS dim_crm_account_id,
      crm_accounts.crm_account_name,
      CASE
        WHEN instance_type = 'Production' THEN 1
        WHEN instance_type = 'Non-Production' THEN 2
        WHEN instance_type = 'Unknown' THEN 3
        ELSE 4
      END                                                                                                            AS instance_type_ordering_field,
      CASE
        WHEN included_in_health_measures_str = 'Included in Health Score' THEN 1
        WHEN included_in_health_measures_str = 'Opt-Out' THEN 2
        WHEN included_in_health_measures_str = NULL THEN 3
      END                                                                                                           AS health_score_ordering_field
    FROM gainsight_instance_info
    LEFT JOIN crm_accounts
      ON gainsight_instance_info.crm_account_id = crm_accounts.dim_crm_account_id
    LEFT JOIN prep_host
      ON gainsight_instance_info.instance_hostname = prep_host.host_name
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@michellecooper",
    created_date="2021-04-01",
    updated_date="2024-05-12"
) }}
