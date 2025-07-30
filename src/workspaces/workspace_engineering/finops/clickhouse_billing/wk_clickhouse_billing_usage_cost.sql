{{ config({
        "materialized": "table",
        "transient": false
    })
}}

WITH base AS (

  SELECT *
  FROM {{ ref("clickhouse_billing_usage_cost") }}

)

  SELECT *
  FROM base