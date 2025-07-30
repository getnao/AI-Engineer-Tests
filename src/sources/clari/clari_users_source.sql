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
    LATERAL FLATTEN(input => jsontext['data']['users']) AS d
),

new_logo_intermediate AS (
  SELECT
    d.value,
    source.uploaded_at
  FROM
    new_logo_source AS source,
    LATERAL FLATTEN(input => jsontext['data']['users']) AS d
),

combined_intermediate AS (
  SELECT * FROM net_arr_intermediate
  UNION ALL
  SELECT * FROM new_logo_intermediate
),

parsed AS (
  SELECT
    -- primary key
    value['userId']::VARCHAR              AS user_id,
    -- logical info
    value['crmId']::VARCHAR               AS crm_user_id,
    value['email']::VARCHAR               AS user_email,
    value['parentHierarchyId']::VARCHAR   AS parent_role_id,
    value['parentHierarchyName']::VARCHAR AS parent_role,
    value['hierarchyId']::VARCHAR         AS sales_team_role_id,
    value['hierarchyName']::VARCHAR       AS sales_team_role,
    value['name']::VARCHAR                AS user_full_name,
    value['scopeId']::VARIANT             AS scope_id,
    uploaded_at
  FROM
    combined_intermediate
  -- remove dups in case of overlapping data from daily/quarter loads
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        user_id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT *
FROM
  parsed
