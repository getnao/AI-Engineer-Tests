{{ config(
    tags=["product", "mnpi_exception"],
    snowflake_warehouse=generate_warehouse_name('XL')
) }}

SELECT *
FROM {{ ref('subscription_product_usage_data') }}
