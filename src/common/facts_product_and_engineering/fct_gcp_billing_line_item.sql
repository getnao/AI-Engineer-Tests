{{ config(
    materialized='incremental',
    unique_key='gcp_billing_line_item_pk',
    on_schema_change='append_new_columns',
    full_refresh=only_force_full_refresh()
    )
}}

WITH source AS (

  SELECT *
  FROM {{ ref('summary_gcp_billing_source') }}
  {% if is_incremental() %}

    WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{ this }})

  {% endif %}

),

credits AS (

  SELECT
    source_primary_key,
    SUM(COALESCE(credit_amount, 0)) AS total_credits
  FROM {{ ref('prep_gcp_billing_credit') }}
  WHERE LOWER(credit_description) != 'migration-credit-1-1709765302259' --excluding migration credit between march 7th and March 27th: https://gitlab.com/gitlab-org/quality/engineering-analytics/finops/finops-analysis/-/issues/142
  GROUP BY 1

),

renamed AS (

  SELECT
    source.primary_key                               AS gcp_billing_line_item_pk,
    source.billing_account_id,
    source.service_id                                AS gcp_service_id,
    source.service_description                       AS gcp_service_description,
    source.sku_id                                    AS gcp_sku_id,
    source.sku_description                           AS gcp_sku_description,
    source.invoice_month,
    source.usage_start_time,
    source.usage_end_time,
    source.project_id                                AS gcp_project_id,
    source.project_name                              AS gcp_project_name,
    source.project_labels                            AS gcp_project_labels,
    source.folder_id,
    source.resource_location,
    source.resource_zone,
    source.resource_region,
    source.resource_country,
    source.labels                                    AS resource_labels,
    source.system_labels,
    source.cost                                      AS cost_before_credits,
    credits.total_credits,
    source.cost + COALESCE(credits.total_credits, 0) AS net_cost,
    source.usage_amount,
    source.usage_unit,
    source.usage_amount_in_pricing_units,
    source.pricing_unit,
    source.currency,
    source.currency_conversion_rate,
    source.cost_type,
    source.credits,
    source.export_time,
    source.uploaded_at
  FROM source
  LEFT JOIN credits
    ON source.primary_key = credits.source_primary_key

)

SELECT *
FROM renamed
