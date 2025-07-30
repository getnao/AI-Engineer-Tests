{{config(

    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=['product'],
    on_schema_change='sync_all_columns',
    pre_hook = ["{{'USE WAREHOUSE ' ~ generate_warehouse_name('4XL') ~ ';' if not is_incremental() }}"],
    post_hook=["{{ rolling_window_delete('behavior_at','day',90) }}", "{{'USE WAREHOUSE ' ~ generate_warehouse_name('XL') ~ ';' if not is_incremental() }}"],
    cluster_by=['behavior_at::DATE']
  )

}}

WITH structured_event_90_days AS (

    SELECT
    {{
      dbt_utils.star(from=ref('fct_behavior_structured_event'),
      except=[
        'CREATED_BY',
        'UPDATED_BY',
        'MODEL_CREATED_DATE',
        'MODEL_UPDATED_DATE',
        'DBT_CREATED_AT',
        'DBT_UPDATED_AT'
        ])
    }}
  FROM {{ ref('fct_behavior_structured_event') }}
  WHERE DATE_TRUNC(DAY, behavior_at) >= DATEADD(DAY, -90, DATE_TRUNC(DAY, CURRENT_DATE))
    {% if is_incremental() %}
      AND behavior_at >= (SELECT MAX(behavior_at) FROM {{ this }})
    {% endif %}

)

{{ dbt_audit(
    cte_ref="structured_event_90_days",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2024-05-03",
    updated_date="2024-05-31"
) }}
