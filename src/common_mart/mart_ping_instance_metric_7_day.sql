{{ config(
    tags=["product", "mnpi_exception"],
    snowflake_warehouse=generate_warehouse_name('XL')
) }}

{{ macro_mart_ping_instance_metric('fct_ping_instance_metric_7_day') }}
