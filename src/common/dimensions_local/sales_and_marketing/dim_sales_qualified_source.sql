{{ config({
    "tags": ["mnpi_exception"],
    "post-hook": "{{ missing_member_column(primary_key = 'dim_sales_qualified_source_id', not_null_test_cols = []) }}"
}) }}

WITH sales_qualified_source AS (

    SELECT
      dim_sales_qualified_source_id,
      sales_qualified_source_name,
      sales_qualified_source_grouped,
      sqs_bucket_engagement
    FROM {{ ref('prep_sales_qualified_source') }}

)

{{ dbt_audit(
    cte_ref="sales_qualified_source",
    created_by="@paul_armstrong",
    updated_by="@chrissharp",
    created_date="2020-10-26",
    updated_date="2025-03-19"
) }}
