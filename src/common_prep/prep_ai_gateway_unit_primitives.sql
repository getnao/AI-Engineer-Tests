{{ config(

    materialized='incremental',
    unique_key='cloud_connector_configuration_pk'
  )

}}

WITH cloud_connector_configuration AS (

  SELECT *
  FROM {{ ref('cloud_connector_configuration_source') }}

),

{% if is_incremental() %}

  id_uploaded_date AS (

    SELECT
      cloud_connector_configuration_snapshot_id,
      valid_from
    FROM {{ this }}
    WHERE valid_to IS NULL

  ),

{% endif %}

source AS (

  SELECT
    cloud_connector_configuration.*,
    TO_TIMESTAMP(snapshot_date)                             AS uploaded_at,
    {{ 
         dbt_utils.generate_surrogate_key(
           [
               'unit_primitive_name',
               'unit_primitive_description',
               'feature_category',
               'engineering_group',
               'backend_services',
               'available_to_license_types',
               'available_to_add_ons',
               'cut_off_date',
               'documentation_url',
               'introduced_by_url',
               'unit_primitive_issue_url',
               'deprecated_by_url',
               'deprecation_message',
               'min_gitlab_version',
               'min_gitlab_version_for_free_access'
           ]
         ) 
    }} AS record_checksum
  FROM cloud_connector_configuration

  {% if is_incremental() %}

    LEFT JOIN id_uploaded_date
      ON cloud_connector_configuration.cloud_connector_configuration_snapshot_id = id_uploaded_date.cloud_connector_configuration_snapshot_id
        AND cloud_connector_configuration.snapshot_date = id_uploaded_date.valid_from
    WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{ this }})
      OR id_uploaded_date.cloud_connector_configuration_snapshot_id IS NOT NULL

  {% endif %}

),

base AS (

  SELECT
    *,
    LEAD(snapshot_date) OVER (PARTITION BY cloud_connector_configuration_snapshot_id ORDER BY snapshot_date)         AS nextsnapshot_date,
    LAG(record_checksum, 1, '') OVER (PARTITION BY cloud_connector_configuration_snapshot_id ORDER BY snapshot_date) AS lag_checksum,
    CONDITIONAL_TRUE_EVENT(record_checksum != lag_checksum)
      OVER (PARTITION BY cloud_connector_configuration_snapshot_id ORDER BY snapshot_date)                           AS checksum_group
  FROM source

),

grouped AS (

  SELECT
    cloud_connector_configuration_snapshot_id,
    unit_primitive_name,
    unit_primitive_description,
    feature_category,
    engineering_group,
    backend_services,
    available_to_license_types,
    available_to_add_ons,
    cut_off_date,
    documentation_url,
    introduced_by_url,
    unit_primitive_issue_url,
    deprecated_by_url,
    deprecation_message,
    min_gitlab_version,
    min_gitlab_version_for_free_access,
    MIN(uploaded_at)                 AS uploaded_at,
    TO_TIMESTAMP(MIN(snapshot_date)) AS valid_from,
    IFF(
      MAX(COALESCE(nextsnapshot_date, '9999-01-01') = '9999-01-01'),
      NULL, TO_TIMESTAMP(MAX(nextsnapshot_date))
    )                                AS valid_to
  FROM base
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, checksum_group

)

SELECT
  *,
  {{
      dbt_utils.generate_surrogate_key(
        [
          'cloud_connector_configuration_snapshot_id',
          'valid_from'
        ]
      ) 
  }}                                  AS cloud_connector_configuration_pk,
  IFF(valid_to IS NULL, TRUE, FALSE)  AS is_currently_valid
FROM grouped