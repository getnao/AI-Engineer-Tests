{{ config(
    tags=["product", "mnpi_exception"],
    pre_hook = ["{{'USE WAREHOUSE ' ~ generate_warehouse_name('4XL') ~ ';' if not is_incremental() }}"],
    post_hook = ["{{'USE WAREHOUSE ' ~ generate_warehouse_name('XL') ~ ';' if not is_incremental() }}"]
) }}

{{ macro_mart_ping_instance_metric('fct_ping_instance_metric_all_time') }}
