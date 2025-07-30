{{ config(
    tags=["mnpi_exception"]
) }}

WITH source_data AS (

    SELECT *
    FROM {{ref('prep_crm_opportunity')}}
    WHERE sales_qualified_source IS NOT NULL
      AND is_live

), sheetload AS (

    SELECT DISTINCT
      sales_qualified_source,
      {{ sales_qualified_source_grouped('sales_qualified_source') }}               AS sales_qualified_source_grouped,
      {{ sqs_bucket_engagement('sales_qualified_source') }}                        AS sqs_bucket_engagement
    FROM {{ref('sheetload_sales_targets_source')}}
    WHERE sales_qualified_source IS NOT NULL

), unioned AS (

    SELECT DISTINCT
      MD5(CAST(COALESCE(CAST(sales_qualified_source AS varchar), '') AS varchar))  AS dim_sales_qualified_source_id,
      sales_qualified_source                                                       AS sales_qualified_source_name,
      sales_qualified_source_grouped                                               AS sales_qualified_source_grouped,
      sqs_bucket_engagement
    FROM source_data

    UNION

    SELECT
      MD5(CAST(COALESCE(CAST(sales_qualified_source AS varchar), '') AS varchar))  AS dim_sales_qualified_source_id,
      sales_qualified_source                                                       AS sales_qualified_source_name,
      sales_qualified_source_grouped                                               AS sales_qualified_source_grouped,
      sqs_bucket_engagement
    FROM sheetload

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@mcooperDD",
    updated_by="@chrissharp",
    created_date="2020-10-26",
    updated_date="2025-01-22"
) }}
