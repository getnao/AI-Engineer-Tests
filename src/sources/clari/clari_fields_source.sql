{{ config(
    materialized="table"
    )
}}

WITH
net_arr_source AS (
  SELECT * FROM
    {{ source('clari', 'net_arr') }}
),

new_logo_source AS (
  SELECT * FROM
    {{ source('clari', 'new_logo') }}
),

net_arr_intermediate AS (
  SELECT
    d.value,
    source.uploaded_at
  FROM
    net_arr_source AS source,
    LATERAL FLATTEN(input => jsontext['data']['fields']) AS d
),

new_logo_intermediate AS (
  SELECT
    d.value,
    source.uploaded_at
  FROM
    new_logo_source AS source,
    LATERAL FLATTEN(input => jsontext['data']['fields']) AS d
),

combined_intermediate AS (
  SELECT * FROM net_arr_intermediate
  UNION ALL
  SELECT * FROM new_logo_intermediate
),

parsed AS (
  SELECT
    value['fieldId']::VARCHAR   AS field_id,
    value['fieldName']::VARCHAR AS field_name,
    value['fieldType']::VARCHAR AS field_type,
    uploaded_at
  FROM
    combined_intermediate
  -- remove dups in case of overlapping data from daily/quarter loads
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        field_id
      ORDER BY
        uploaded_at DESC
    ) = 1
  ORDER BY field_id
),

renamed AS (
  SELECT
    field_id,
    CASE WHEN field_id = 'fc_50_50' THEN 'Professional Services Most Likely'
      WHEN field_id = 'fc_best_case' THEN 'Professional Services Best Case'
      ELSE field_name
    END AS field_name,
    field_type,
    uploaded_at
  FROM parsed
)

SELECT *
FROM
  renamed
